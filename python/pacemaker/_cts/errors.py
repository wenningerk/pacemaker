"""A module providing custom exception classes used throughout the pacemaker library."""

__all__ = ["ExitCodeError", "OutputFoundError", "OutputNotFoundError", "XmlValidationError"]
__copyright__ = "Copyright 2009-2024 the Pacemaker project contributors"
__license__ = "GNU General Public License version 2 or later (GPLv2+)"


class TestError(Exception):
    """Base class for exceptions in this module."""


class ExitCodeError(TestError):
    """Exception raised when command exit status is unexpected."""

    def __init__(self, exit_code):
        """Create a new ExitCodeError instance."""
        TestError.__init__(self)
        self.exit_code = exit_code

    def __str__(self):
        """Return a printable string for this exception."""
        return repr(self.exit_code)


class OutputNotFoundError(TestError):
    """Exception raised when command output does not contain wanted string."""

    def __init__(self, output):
        """Create a new OutputNotFoundError instance."""
        TestError.__init__(self)
        self.output = output

    def __str__(self):
        """Return a printable string for this exception."""
        return repr(self.output)


class OutputFoundError(TestError):
    """Exception raised when command output contains unwanted string."""

    def __init__(self, output):
        """Create a new OutputFoundError instance."""
        TestError.__init__(self)
        self.output = output

    def __str__(self):
        """Return a printable string for this exception."""
        return repr(self.output)


class XmlValidationError(TestError):
    """Exception raised when xmllint fails."""

    def __init__(self, output, exit_code):
        """Create a new XmlValidationError instance."""
        TestError.__init__(self)
        self.exit_code = exit_code
        self.output = output

    def __str__(self):
        """Return a printable string for this exception."""
        return repr(self.output)
