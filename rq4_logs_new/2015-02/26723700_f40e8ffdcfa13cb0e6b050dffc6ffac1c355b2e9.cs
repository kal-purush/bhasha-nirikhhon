using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsFormsApplication1.Classes
{
    public class DrawVisitor : IDrawElementVisitor
    {
        private Graphics _g;
        private Pen _color;

        public DrawVisitor(Graphics g, Pen color)
        {
            _g = g;
            _color = color;
        }

        public void visit(Group group)
        {
            //foreach (Graphic g in group.getGraphics())
            //    g.accept(this);
        }

        public void visit(BasicShape basic)
        {
            basic.Draw(_g, _color);
        }


        public void end_visit(Group group)
        {
            // Dont do anything
        }
    }

    public class ResizeVisitor : IDrawElementVisitor
    {
        int _x, _y, _width, _height;
        float _yScaled, _xScaled;
        int _top, _left, _change_x, _change_y;

        public ResizeVisitor(int x, int y, int width, int height, Graphic shape)
        {
            _x = x;
            _y = y;
            _width = width;
            _height = height;

            // Calculate scalling etc done
            _yScaled = height / (float)shape.getHeight();
            _top = shape.getTop();
            _xScaled = width / (float)shape.getWidth();
            _left = shape.getLeft();
            _change_y = y - shape.getTop();
            _change_x = x - shape.getLeft();
        }

        public void visit(Group group)
        {

        }

        public void end_visit(Group group)
        {

        }

        public void visit(BasicShape basic)
        {
            // Calculate translation
            basic.setY(basic.getTop() + _change_y);
            basic.setX(basic.getLeft() + _change_x);

            // Calculate new height
            basic.setHeight((int)(basic.getHeight() * _yScaled));

            // Calculate new width
            basic.setWidth((int)(basic.getWidth() * _xScaled));
        }
    }

    public class SaveVisitior : IDrawElementVisitor
    {
        int _depth;
        string _out;

        public SaveVisitior(int depth)
        {
            _depth = depth;
            _out = "";
        }

        public string getString()
        {
            return _out;
        }

        public void visit(Group group)
        {
            _out += String.Format("{0}group {1}" + Environment.NewLine, new String(' ', _depth), group.getGraphics().Count());
            _depth += 1;
            //foreach (Graphic g in group.getGraphics())
            //    g.accept(this);
            //_depth -= 1;
        }

        public void visit(BasicShape basic)
        {
            _out += String.Format(
                "{4}{5} {0} {1} {2} {3}" + Environment.NewLine,
                basic.getLeft(), basic.getTop(), basic.getWidth(), basic.getHeight(), new String(' ', _depth), basic.toString()
            );
        }

        public void end_visit(Group group)
        {
            _depth -= 1;
        }
    }
}