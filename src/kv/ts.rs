use std::marker::PhantomData;

pub struct Unset<T>(PhantomData<T>);

impl<T> Unset<T> {
    pub(super) fn new() -> Self {
        Self(PhantomData)
    }
}

pub struct Ignored<T>(PhantomData<T>);

impl<T> Ignored<T> {
    pub(super) fn new() -> Self {
        Self(PhantomData)
    }
}
pub struct Set<T>(pub(crate) T);

impl From<Unset<bool>> for bool {
    fn from(_: Unset<bool>) -> Self {
        false
    }
}

impl From<Set<()>> for bool {
    fn from(_: Set<()>) -> Self {
        true
    }
}

impl<T> From<Set<T>> for Option<T> {
    fn from(value: Set<T>) -> Self {
        Some(value.0)
    }
}

impl<T> From<Unset<T>> for Option<T> {
    fn from(_: Unset<T>) -> Self {
        None
    }
}

impl<T> From<Ignored<T>> for Option<T> {
    fn from(_: Ignored<T>) -> Self {
        None
    }
}

pub enum OptionOrIgnored<T> {
    Some(T),
    None,
    Ignored,
}

impl<T> From<Set<T>> for OptionOrIgnored<T> {
    fn from(value: Set<T>) -> Self {
        OptionOrIgnored::Some(value.0)
    }
}

impl<T> From<Unset<T>> for OptionOrIgnored<T> {
    fn from(_: Unset<T>) -> Self {
        OptionOrIgnored::None
    }
}

impl<T> From<Ignored<T>> for OptionOrIgnored<T> {
    fn from(_: Ignored<T>) -> Self {
        OptionOrIgnored::Ignored
    }
}
