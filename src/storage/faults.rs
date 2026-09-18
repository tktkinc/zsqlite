//! Test-only publication-boundary injection; absent from production builds.
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Point {
    BootstrapDataSynced,
    BootstrapClaimed,
    BootstrapInstalled,
    ObjectDataSynced,
    ObjectLinked,
    ObjectDirectorySynced,
    ManifestReady,
    ActiveDataSynced,
    ActiveRenamed,
    ActiveDirectorySynced,
    ActiveStateWritten,
    ActiveStateSynced,
    RootRenamed,
    RootDirectorySynced,
}
#[derive(Clone, Copy)]
pub(crate) enum Mode {
    Error,
    Crash,
}
thread_local! { static INJECTION: RefCell<Option<(Point, Mode)>> = const { RefCell::new(None) }; }
#[must_use]
pub(crate) struct Injection;
pub(crate) fn inject(point: Point, mode: Mode) -> Injection {
    INJECTION.with(|slot| *slot.borrow_mut() = Some((point, mode)));
    Injection
}
impl Drop for Injection {
    fn drop(&mut self) {
        INJECTION.with(|slot| *slot.borrow_mut() = None);
    }
}
pub(crate) fn check(point: Point) -> std::io::Result<()> {
    let mode = INJECTION.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|(selected, _)| *selected == point)
        {
            slot.take().map(|(_, mode)| mode)
        } else {
            None
        }
    });
    match mode {
        None => Ok(()),
        Some(Mode::Error) => Err(std::io::Error::other(format!("injected {point:?}"))),
        Some(Mode::Crash) => std::process::exit(73),
    }
}
