use std::{
    sync::Arc,
    task::{Context, Poll},
};

use arc_swap::ArcSwap;
use http::{header::AUTHORIZATION, HeaderValue, Request};
use tower::Service;

type SharedToken = Arc<ArcSwap<Option<HeaderValue>>>;

pub struct AuthServiceTokenSetter {
    token: SharedToken,
}

impl AuthServiceTokenSetter {
    pub fn set_token(&self, token: HeaderValue) {
        self.token.store(Arc::new(Some(token)));
    }

    pub fn clear_token(&self) {
        self.token.store(Arc::new(None));
    }
}

#[derive(Debug, Clone)]
pub struct AuthService<S> {
    inner: S,
    token: SharedToken,
}

impl<S> AuthService<S> {
    pub(crate) fn pair(inner: S, token: Option<HeaderValue>) -> (Self, AuthServiceTokenSetter) {
        let token = Arc::new(ArcSwap::new(Arc::new(token)));
        let setter = AuthServiceTokenSetter {
            token: token.clone(),
        };
        let service = Self { inner, token };

        (service, setter)
    }
}

impl<S, Body, Response> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        if let Some(token) = &**self.token.load() {
            request.headers_mut().insert(AUTHORIZATION, token.clone());
        }
        self.inner.call(request)
    }
}
