// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

#[cfg(tokio_unstable)]
use crate::logger::log_trace;
use crate::logger::{log_error, LdkLogger, Logger};

use tokio::task::JoinHandle;

use std::fmt;
use std::future::Future;
use std::sync::{Arc, RwLock};

pub(crate) struct Runtime {
	state: RwLock<RuntimeState>,
	logger: Arc<Logger>,
}

impl Runtime {
	pub fn new(logger: Arc<Logger>) -> Self {
		let state = RwLock::new(RuntimeState::Stopped);
		Self { state, logger }
	}

	pub fn start(&self) -> Result<(), RuntimeError> {
		let mut state_lock = self.state.write().unwrap();
		if !matches!(*state_lock, RuntimeState::Stopped) {
			return Err(RuntimeError::AlreadyRunning);
		}
		match tokio::runtime::Handle::try_current() {
			Ok(handle) => *state_lock = RuntimeState::Handle(handle),
			Err(_) => {
				let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(
					|e| {
						log_error!(self.logger, "Failed to setup tokio runtime: {}", e);
						RuntimeError::SetupFailed
					},
				)?;
				*state_lock = RuntimeState::Owned(rt)
			},
		}
		Ok(())
	}

	pub fn start_from_handle(&self, handle: tokio::runtime::Handle) -> Result<(), RuntimeError> {
		let mut state_lock = self.state.write().unwrap();
		if !matches!(*state_lock, RuntimeState::Stopped) {
			return Err(RuntimeError::AlreadyRunning);
		}
		*state_lock = RuntimeState::Handle(handle);
		Ok(())
	}

	pub fn stop(&self) -> Result<(), RuntimeError> {
		let mut state_lock = self.state.write().unwrap();
		if matches!(*state_lock, RuntimeState::Stopped) {
			return Err(RuntimeError::NotRunning);
		}

		let old_state = core::mem::replace(&mut *state_lock, RuntimeState::Stopped);
		match old_state {
			RuntimeState::Owned(rt) => {
				#[cfg(tokio_unstable)]
				log_trace!(
					self.logger,
					"Active runtime tasks left prior to shutdown: {}",
					rt.metrics().active_tasks_count()
				);
				rt.shutdown_background();
			},
			RuntimeState::Handle(_handle) => {
				#[cfg(tokio_unstable)]
				log_trace!(
					self.logger,
					"Active runtime tasks left prior to shutdown: {}",
					_handle.metrics().active_tasks_count()
				);
			},
			RuntimeState::Stopped => return Err(RuntimeError::NotRunning),
		}

		Ok(())
	}

	pub fn is_running(&self) -> bool {
		!matches!(*self.state.read().unwrap(), RuntimeState::Stopped)
	}

	pub fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, RuntimeError>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		let handle = self.handle()?;
		Ok(handle.spawn(future))
	}

	pub fn spawn_blocking<F, R>(&self, func: F) -> Result<JoinHandle<R>, RuntimeError>
	where
		F: FnOnce() -> R + Send + 'static,
		R: Send + 'static,
	{
		let handle = self.handle()?;
		Ok(handle.spawn_blocking(func))
	}

	pub fn block_on<F: Future>(&self, future: F) -> Result<F::Output, RuntimeError> {
		let handle = self.handle()?;
		Ok(tokio::task::block_in_place(move || handle.block_on(future)))
	}

	fn handle(&self) -> Result<tokio::runtime::Handle, RuntimeError> {
		match &*self.state.read().unwrap() {
			RuntimeState::Owned(rt) => Ok(rt.handle().clone()),
			RuntimeState::Handle(handle) => Ok(handle.clone()),
			RuntimeState::Stopped => Err(RuntimeError::NotRunning),
		}
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		let _ = self.stop();
	}
}

enum RuntimeState {
	Owned(tokio::runtime::Runtime),
	Handle(tokio::runtime::Handle),
	Stopped,
}

#[derive(Debug)]
pub(crate) enum RuntimeError {
	SetupFailed,
	AlreadyRunning,
	NotRunning,
}

impl std::error::Error for RuntimeError {}

impl fmt::Display for RuntimeError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Self::AlreadyRunning => write!(f, "Runtime already running."),
			Self::NotRunning => write!(f, "Runtime is not running."),
			Self::SetupFailed => write!(f, "Failed to setup runtime."),
		}
	}
}

impl From<RuntimeError> for std::io::Error {
	fn from(runtime_error: RuntimeError) -> Self {
		let msg = format!("Runtime error: {}", runtime_error);
		std::io::Error::new(std::io::ErrorKind::Other, msg)
	}
}
