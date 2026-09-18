//! Test-only publication-boundary injection; absent from production builds.
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
    ObjectRemoved,
    ObjectDeletionSynced,
}
#[derive(Clone, Copy)]
pub(crate) enum Mode {
    Error,
    Crash,
}
thread_local! {
    static INJECTION: RefCell<Option<(Point, Mode, usize)>> = const { RefCell::new(None) };
    static RECORDING: RefCell<Option<Vec<Point>>> = const { RefCell::new(None) };
}
#[must_use]
pub(crate) struct Injection;
pub(crate) fn inject(point: Point, mode: Mode) -> Injection {
    inject_nth(point, mode, 1)
}
pub(crate) fn inject_nth(point: Point, mode: Mode, occurrence: usize) -> Injection {
    assert!(occurrence > 0);
    INJECTION.with(|slot| *slot.borrow_mut() = Some((point, mode, occurrence)));
    Injection
}

/// Discover every occurrence in a real successful operation before crashing
/// each one. New object writes or publication steps then extend the matrix.
#[must_use]
pub(crate) struct Recording;
pub(crate) fn record() -> Recording {
    RECORDING.with(|slot| {
        assert!(slot.borrow().is_none(), "nested fault recording");
        *slot.borrow_mut() = Some(Vec::new());
    });
    Recording
}
impl Recording {
    pub(crate) fn finish(self) -> Vec<Point> {
        let points = RECORDING.with(|slot| slot.borrow_mut().take().expect("active recording"));
        drop(self);
        points
    }
}
impl Drop for Recording {
    fn drop(&mut self) {
        RECORDING.with(|slot| *slot.borrow_mut() = None);
    }
}
impl Drop for Injection {
    fn drop(&mut self) {
        INJECTION.with(|slot| *slot.borrow_mut() = None);
    }
}
pub(crate) fn check(point: Point) -> std::io::Result<()> {
    RECORDING.with(|slot| {
        if let Some(points) = slot.borrow_mut().as_mut() {
            points.push(point);
        }
    });
    let mode = INJECTION.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some((selected, _, remaining)) = slot.as_mut()
            && *selected == point
        {
            *remaining -= 1;
            if *remaining == 0 {
                return slot.take().map(|(_, mode, _)| mode);
            }
        }
        None
    });
    match mode {
        None => Ok(()),
        Some(Mode::Error) => Err(std::io::Error::other(format!("injected {point:?}"))),
        Some(Mode::Crash) => std::process::exit(73),
    }
}
