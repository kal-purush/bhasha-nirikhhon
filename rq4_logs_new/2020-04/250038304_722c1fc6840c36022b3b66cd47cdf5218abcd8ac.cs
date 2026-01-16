using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ODWai2.Misc;
using System.Windows.Forms;
using System.Drawing;
using ODWai2.Misc.Classes;

namespace ODWai2.Controllers
{
    class ClassCreatePanel
    {
        Misc.Views.NewInputSetView NewInputSetView;
        public ClassCreatePanel(Misc.Views.NewInputSetView form)
        {
            NewInputSetView = form;
        }

        public InputGroup Create(int s)
        {
            var p = new Panel { BackColor = Color.Azure, Size = NewInputSetView.pnl_0.Size, Name = "pnl_" + s.ToString() };
            Label _lbfield = null, _lb_sample = null, _lb_error = null, _lb_associated = null;
            TextBox _txt_field = null, _txt_associated = null, _txt_sample = null, _txt_error = null;
            CheckBox crr = new CheckBox();

            add_stuff(ref _lbfield, ref _txt_field, "Field", "lb_field", "txt_field_" + s.ToString(), NewInputSetView.lb_2cham_1.Location, NewInputSetView.lb_2cham_1.Size, NewInputSetView.lb_2cham_1.Font, NewInputSetView.lb_field_0, NewInputSetView.txt_field_0);
            add_stuff(ref _lb_associated, ref _txt_associated, "Associated texts", "lb_associated", "txt_associated_" + s.ToString(), NewInputSetView.lb_2cham_2.Location, NewInputSetView.lb_2cham_2.Size, NewInputSetView.lb_2cham_2.Font, NewInputSetView.lb_associal_0, NewInputSetView.txt_associated_0);
            add_stuff(ref _lb_sample, ref _txt_sample, "Sample input", "lb_sample", "txt_sample_" + s.ToString(), NewInputSetView.lb_2cham_3.Location, NewInputSetView.lb_2cham_3.Size, NewInputSetView.lb_2cham_3.Font, NewInputSetView.lb_sample_0, NewInputSetView.txt_sampleInput_0);
            add_stuff(ref _lb_error, ref _txt_error, "Error input", "lb_error", "txt_error_" + s.ToString(), NewInputSetView.lb_2cham_4.Location, NewInputSetView.lb_2cham_4.Size, NewInputSetView.lb_2cham_4.Font, NewInputSetView.lb_error_0, NewInputSetView.txt_errorInput_0);
            p.Controls.Add(crr = new CheckBox { Name = "chbox_" + s.ToString(), Location = NewInputSetView.chbox_0.Location, Size = NewInputSetView.chbox_0.Size });

            void add_stuff(ref Label lbfield, ref TextBox txt_field, string label_text, string label_name, string txt_name, Point colon_location, Size colon_size, Font colon_font, Label label_ref, TextBox txt_ref)
            {
                p.Controls.Add(lbfield = new Label { Text = label_text, Name = label_name, Location =label_ref.Location, Size = label_ref.Size, Font = label_ref.Font, AutoSize = true });
                p.Controls.Add(new Label { Text = ":", Location = colon_location, Size = colon_size, Font = colon_font });
                p.Controls.Add(txt_field = new TextBox { Name = txt_name + s.ToString(), Size = txt_ref.Size, Location = txt_ref.Location, Font = txt_ref.Font });
            }

            return new InputGroup() {
                arr_ = p, crr_ = crr, lbfield_ = _lbfield, error_ = _txt_error,
                associated_ = _txt_associated, sample_ = _txt_sample,
                field_ = _txt_field, lbassociated_ = _lb_associated,
                lberror_ = _lb_error, lbsample_ = _lb_sample
            };
        }
    }
}