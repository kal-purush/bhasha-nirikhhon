using System.Collections.Generic;

namespace HolisticWare.Xamarin.Android.Bindings.Tools.NeekNoke;

public partial class ResultsPerFormat
{
    public 
        Dictionary
                <
                    string,                     // format regex
                    Formats.NeekerNokerBase
                >
                                        ResultsNeekerNoker
    {
        get;
        set;
    }

    public 
        Dictionary
                <
                    string,                     // format regex
                    ResultsPerFile
                >
                                        ResultsPerFile
    {
        get;
        set;
    }

    public 
                                        ResultsPerFormat
                                            (
                                            )
    {
        this.ResultsNeekerNoker = new Dictionary
                                            <
                                                string,                     // format / pattern
                                                Formats.NeekerNokerBase
                                            >
                                            ();

        this.ResultsPerFile = new Dictionary
                                            <
                                                string,                     // file
                                                ResultsPerFile
                                            >
                                            ();

        return;
    }
}