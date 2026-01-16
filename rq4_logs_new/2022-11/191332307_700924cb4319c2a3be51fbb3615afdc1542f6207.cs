using System.Collections.Generic;

namespace HolisticWare.Xamarin.Android.Bindings.Tools.NeekNoke;

public partial class ResultsPerPattern
{
    public 
        Dictionary
                <
                    string,                     // format regex
                    Formats.NeekerNokerBase
                >
                                        Results
    {
        get;
        set;
    }

    public 
                                        ResultsPerPattern
                                            (
                                            )
    {
        this.Results = new Dictionary
                                <
                                    string,                     // format regex
                                    Formats.NeekerNokerBase
                                >
                                ();

        return;
    }
}