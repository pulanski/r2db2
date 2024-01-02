use codespan_reporting::diagnostic::Diagnostic;

/// A trait for objects that can emit diagnostics.
///
/// `Emittable` is intended to be implemented by error types across different subsystems
/// of a SQL processing application, such as parsing, semantic analysis, and binding.
/// Implementing this trait allows each error type to define how it should be reported
/// as a diagnostic message, ensuring consistency and clarity in error reporting across
/// the system.
///
/// Each subsystem can define its own error types, and by implementing `Emittable`,
/// these errors can be converted into a `Diagnostic` object. This `Diagnostic` object
/// can then be used with `codespan_reporting`'s reporting facilities to display rich,
/// informative error messages to the end-user.
///
/// Implementors of this trait should focus on creating a meaningful `Diagnostic` that
/// accurately represents the error state. This includes setting appropriate error
/// messages, error codes, and highlighting relevant parts of the input source code
/// where the error occurred.
///
/// # Examples
///
/// Implementing `Emittable` for a custom syntax error type might look like this:
///
/// ```
/// struct SyntaxError {
///     message: String,
///     // Other fields like error location, severity, etc.
/// }
///
/// impl Emittable for SyntaxError {
///     fn emit_diagnostic(&self) -> Diagnostic<()> {
///         Diagnostic::error()
///             .with_message(&self.message)
///             // Additional diagnostic configurations like setting labels,
///             // secondary messages, etc., based on error details.
///     }
/// }
/// ```
///
/// Once implemented, `SyntaxError` can then be converted into a `Diagnostic`
/// and reported using `codespan_reporting`'s terminal emitter or other reporting
/// mechanisms provided by the library.
pub trait Emittable {
    /// Converts the implementing error type into a `Diagnostic`.
    ///
    /// This method should encapsulate the logic to create a `Diagnostic` object
    /// from the current state of the error. The `Diagnostic` object is used by
    /// `codespan_reporting` to generate rich and informative error messages.
    ///
    /// Returns a `Diagnostic<()>` which can be emitted using `codespan_reporting`'s
    /// reporting facilities.
    ///
    /// # Returns
    ///
    /// A `Diagnostic<()>` object representing the current error state.
    fn emit_diagnostic(&self) -> Diagnostic<()>;
}
