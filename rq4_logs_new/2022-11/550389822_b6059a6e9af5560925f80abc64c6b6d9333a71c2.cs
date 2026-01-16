using System;
using System.Collections.Generic;

namespace Exercism.Sets
{
  public class FacialFeatures
  {
    public string EyeColor { get; }
    public decimal PhiltrumWidth { get; }
    public FacialFeatures(string eyeColor, decimal philtrumWidth) =>
      (this.EyeColor, this.PhiltrumWidth) = (eyeColor, philtrumWidth);

    /// <summary> 値比較のため用です
    ///           参照比較は"=="を使用して下さい
    /// </summary>
    public override bool Equals(object? obj)
    {
      if (obj == null || this.GetType() != obj.GetType()) {
        return false;
      }
      return this.GetHashCode() == obj.GetHashCode();
    }

    public override int GetHashCode()
    {
      // HashCode.Combine(this.EyeColor, this.PhiltrumWidth);
      return EyeColor.GetHashCode() ^ PhiltrumWidth.GetHashCode();
    }
  }

  public class Identity
  {
    public string Email { get; }
    public FacialFeatures FacialFeatures { get; }
    public Identity(string email, FacialFeatures facialFeatures) =>
      (this.Email, this.FacialFeatures) = (email, facialFeatures);

    /// <summary> 値比較のため用です
    ///           参照比較は"=="を使用して下さい
    /// </summary>
    public override bool Equals(object? obj)
    {
      if (obj == null || this.GetType() != obj.GetType()) {
        return false;
      }
      return this.GetHashCode() == obj.GetHashCode();
    }
    public override int GetHashCode()
    {
      return Email.GetHashCode() ^ FacialFeatures.GetHashCode();
    }
  }

  public class Authenticator
  {
    static readonly Identity _admin = new Identity("admin@exerc.ism", new FacialFeatures("green", 0.9m));

    HashSet<int> registered = new HashSet<int>();
    public static bool AreSameFace(FacialFeatures faceA, FacialFeatures faceB)
    {
      return faceA.Equals(faceB);
    }
    public bool IsAdmin(Identity identity) => identity.Equals(_admin); 
    public bool Register(Identity identity)
    {
      return registered.Add(identity.GetHashCode());
    }
    public bool IsRegistered(Identity identity)
    {
      return registered.Contains(identity.GetHashCode());
    }
    public static bool AreSameObject(Identity identityA, Identity identityB)
    {
      // 参照比較のみで十分
      return identityA == identityB;
    }
  }
}
