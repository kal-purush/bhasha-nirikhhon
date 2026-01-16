using System;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotflik.Domain.Exceptions;

namespace Dotflik.Application.Pagination
{
  /// <summary>
  /// Represent the page token for offset pagination
  /// </summary>
  public sealed class OffsetPageToken
  {
    public const string PageTokenFormat = "limit={0}&offset={1}";

    /// <summary>
    /// Dynamically construct the regex so if the <see cref="PageTokenFormat"/> 
    /// ever changes, we'll know to update this field as well
    /// </summary>
    private static readonly string PageTokenRegex = "^" + string.Format(PageTokenFormat, "[0-9]+", "[0-9]+") + "$";

    public int Limit { get; init; }

    public int Offset { get; init; }

    public OffsetPageToken() { }

    public OffsetPageToken(string pageToken)
    {
      if (string.IsNullOrWhiteSpace(pageToken))
      {
        (Limit, Offset) = (0, 0);
        return;
      }

      if (!Regex.IsMatch(pageToken, PageTokenRegex))
      {
        var errorMessage = $"Token '{pageToken}' is not in correct format '{PageTokenRegex}'.";
        throw new BadPageTokenFormatException(errorMessage);
      }

      var tokens = pageToken.Split('&');
      Limit = int.Parse(tokens[0].Split('=')[1]);
      Offset = int.Parse(tokens[1].Split('=')[1]);
    }

    public string ToToken() => string.Format(PageTokenFormat, Limit, Offset);

  }
}