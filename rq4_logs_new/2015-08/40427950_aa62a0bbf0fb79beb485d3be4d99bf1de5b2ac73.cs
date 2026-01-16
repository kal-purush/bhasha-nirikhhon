using System;

namespace GetLocalFilePath.Plugin.Abstractions
{
  /// <summary>
  /// Interface for GetLocalFilePath
  /// </summary>
  public interface IGetLocalFilePath
    {

     string GetLocalPath(string fileName);

    }
}