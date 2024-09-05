pub(crate) fn is_retryable_error_code(code: tonic::Code) -> bool {
    use tonic::Code::*;

    match code {
        Unknown | DeadlineExceeded | ResourceExhausted | Aborted | Internal | Unavailable => true,
        Ok | Cancelled | InvalidArgument | NotFound | AlreadyExists | PermissionDenied
        | FailedPrecondition | OutOfRange | Unimplemented | DataLoss | Unauthenticated => false,
    }
}
