using System.Text.RegularExpressions;

namespace Hangfire.Oracle.Core;

internal static partial class OracleIdentifier
{
    private const int MaxIdentifierLength = 128;

    public static string Validate(string value, string parameterName, bool allowEmpty = false)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            if (allowEmpty)
            {
                return string.Empty;
            }

            throw new ArgumentException("Oracle identifiers cannot be empty.", parameterName);
        }

        if (value.Length > MaxIdentifierLength || !ValidIdentifier().IsMatch(value))
        {
            throw new ArgumentException(
                "Oracle identifiers must start with a letter and contain only letters, digits, _, $, or #.",
                parameterName);
        }

        return value.ToUpperInvariant();
    }

    public static string ValidatePrefix(string value, string parameterName)
    {
        if (value is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        var prefix = Validate(value, parameterName);
        _ = Validate($"IX_{prefix}JOB_PARAMETER_JOB_NAME", parameterName);
        return prefix;
    }

    [GeneratedRegex("^[A-Za-z][A-Za-z0-9_$#]*$", RegexOptions.CultureInvariant)]
    private static partial Regex ValidIdentifier();
}
