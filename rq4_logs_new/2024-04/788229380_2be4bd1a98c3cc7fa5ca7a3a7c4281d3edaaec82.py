
from manim import *
from manim_slides import Slide

config["background_color"] = "#E3BC9A"
Text.set_default(color=BLACK)
MathTex.set_default(color=BLACK)


class Title(Slide):
    def construct(self):
        title = VGroup(
            Tex("Advanced \\LaTeX\\").scale(1.4),
            Text("made by Sherstobitov Andrei").scale(0.8),
            Text("powered by Manim Community & Manim Slides").scale(0.5)
        ).arrange(DOWN)
        self.play(Write(title))
        self.next_slide()


class Topics(Slide):
    def construct(self):
        topic = Text("Today we'll discuss:").to_corner(UP + LEFT)
        self.play(Write(topic))
        self.next_slide()

        b1 = Text("1. How to tex on your computer.").to_edge(
            LEFT).shift(UP)
        self.play(Write(b1))
        self.next_slide()

        b2 = Text("2. How to use cross references.").to_edge(
            LEFT)
        self.play(Write(b2))
        self.next_slide()

        b3 = Text("3. How to add graphics using tex.").to_edge(
            LEFT).shift(DOWN)
        self.play(Write(b3))
        self.next_slide()


class OverleafHate(Slide):
    def construct(self):
        title = Text("Let's consider Overleaf")
        self.play(Write(title))
        self.next_slide()

        before_compile = ImageMobject(
            "assets/images/overleaf_before_compile.png")
        self.play(FadeIn(before_compile))
        self.next_slide()
        self.clear()

        self.play(Write(Text("Waiting..."), run_time=4))
        self.next_slide()
        self.clear()

        timeout_error = ImageMobject(
            "assets/images/overleaf_timeout_error.png")
        self.play(FadeIn(timeout_error))
        self.next_slide()

        subscribe = ImageMobject("assets/images/overleaf_subscribe.png")
        self.play(FadeOut(timeout_error, shift=UP),
                  FadeIn(subscribe, shift=UP))
        self.next_slide()


def MakeCommandLine(cmd):
    return Code(
        code=cmd,
        language="console", line_spacing=0.8,
        style="native",
        background="rectange",
        font_size=20
    )


class InstallLatexMk(Slide):
    def construct(self):
        title = Text("Install latexmk.")
        self.play(Create(title))
        self.next_slide()
        self.clear()

        on_linux = Text("On Linux:").to_corner(UP + LEFT)
        self.play(Write(on_linux))

        perl_check = MakeCommandLine("perl --version")
        perl_check.add(
            Text("1. Perl (www.perl.org) should be already installed.").scale(0.5).next_to(
                perl_check, UP))

        self.play(FadeIn(perl_check, shift=RIGHT))
        self.next_slide()

        install_latexmk = MakeCommandLine("sudo apt install -y latexmk")
        install_latexmk.add(
            Text("2. Install a package called latexmk, i.e. on Ubuntu:").scale(0.5).next_to(
                install_latexmk, UP))

        self.play(Transform(perl_check, install_latexmk))
        self.next_slide()
        self.clear()

        on_macos = Text("On macOS:").to_corner(UP + LEFT)
        self.play(Write(on_macos))

        install_mactex = MakeCommandLine("brew install --cask mactex")
        install_mactex.add(Text(
            """1. Install MacTex either from original
source (https://tug.org/mactex/mactex-download.html) or using brew""").scale(0.5).next_to(install_mactex, UP))

        self.play(FadeIn(install_mactex, shift=RIGHT))
        self.next_slide()

        install_latexmk_byhand = MakeCommandLine("sudo tlmgr install latexmk")
        install_latexmk_byhand.add(
            Text("""2. If not installed, open “TeX Live Utility”,
search for “latexmk” and install it, or if you prefer
using the Terminal:""").scale(0.5).next_to(
                install_latexmk_byhand, UP))

        self.play(Transform(install_mactex, install_latexmk_byhand))
        self.next_slide()
        self.clear()

        topic = Text("On Windows:").to_corner(UP + LEFT)
        self.play(Write(topic))

        steps = VGroup(
            Text("1. Install MikTex: https://miktex.org/howto/install-miktex"),
            Text("2. Install Perl for Windows: https://strawberryperl.com"),
            Text("3. Use the MikTeX Package Manager to install latexmk.")
        ).scale(0.5).align_to(LEFT)
        steps.arrange(DOWN, center=False, aligned_edge=LEFT)
        self.play(Write(steps.shift(2 * LEFT)))


def MakeTexCode(file_name, font_size=20):
    return Code(
        file_name=file_name,
        language="latex",
        line_spacing=0.8,
        style="vim",
        background="window",
        font_size=font_size,
        tab_width=2,
    )


class ReferenceSectionsAndChaptersIncorrectly(Slide):
    def construct(self):
        topic = Text("Reference sections and chapters by hand.").scale(0.7).to_corner(
            UP + LEFT)
        self.play(Create(topic))

        filename_before = "assets/tex/cross-ref-sections-by-hand-before-change.tex"
        code_before = MakeTexCode(filename_before).scale(0.7).to_edge(LEFT)
        self.play(Create(code_before))
        self.next_slide()

        result_before = ImageMobject(
            "assets/images/cross-ref-sections-by-hand-before-change.png").scale(1.1)
        result_before.to_edge(RIGHT)
        self.play(result_before.animate)
        self.next_slide()

        filename_after = "assets/tex/cross-ref-sections-by-hand-after-change.tex"
        code_after = MakeTexCode(filename_after).scale(0.7).to_edge(LEFT)
        result_after = ImageMobject(
            "assets/images/cross-ref-sections-by-hand-after-change.png").scale(1.1)
        result_after.to_edge(RIGHT)

        self.play(Transform(code_before, code_after))
        self.next_slide()
        self.play(Transform(result_before, result_after))
        self.next_slide()

        self.play(
            Indicate(code_after.submobjects[2][15][0:3], scale_factor=3, color=RED))
        self.next_slide()


class ReferenceSectionsAndChapters(Slide):
    def construct(self):
        topic = Text("Reference sections and chapters automatically.").scale(0.7).to_corner(
            UP + LEFT)
        self.play(Create(topic))

        filename = "assets/tex/cross-ref-sections.tex"
        code = MakeTexCode(filename).scale(0.6)
        self.play(code.animate.to_edge(LEFT))
        self.next_slide()

        run_latexmk = MakeCommandLine(
            "latexmk -pdf assets/tex/cross-ref-sections.tex").scale(0.6).to_corner(DOWN + LEFT)
        self.play(Create(run_latexmk))
        self.next_slide()

        result = ImageMobject("assets/images/cross-ref-sections.png")
        self.play(result.animate.to_edge(RIGHT))

        self.next_slide()
        self.play(Indicate(code.submobjects[2][4][24:45]))
        self.play(Indicate(code.submobjects[2][14][11:30]))

        self.next_slide()
        self.play(Indicate(code.submobjects[2][10][32:57]))  # label
        self.play(Indicate(code.submobjects[2][15][0:24]))  # ref


class ReferenceGraphics(Slide):
    def construct(self):
        topic = Text("Reference images.").scale(0.7).to_corner(UP + LEFT)
        self.play(Create(topic))

        filename = "assets/tex/cross-ref-image.tex"
        code = MakeTexCode(filename).scale(0.6)
        self.play(code.animate.to_edge(LEFT))
        self.next_slide()

        run_latexmk = MakeCommandLine(
            "latexmk -pdf assets/tex/cross-ref-image.tex").scale(0.6).to_corner(DOWN + LEFT)
        self.play(Create(run_latexmk))
        self.next_slide()

        result = ImageMobject("assets/images/cross-ref-image.png").scale(1.2)
        self.play(result.animate.to_edge(RIGHT))

        self.next_slide()
        self.play(Indicate(code.submobjects[2][8]))
        self.play(Indicate(code.submobjects[2][12][8:8 + 14]))


class ReferenceEquations(Slide):
    def construct(self):
        topic = Text("Reference equations.").scale(0.7).to_corner(UP + LEFT)
        self.play(Create(topic))

        filename = "assets/tex/cross-ref-eqs.tex"
        code = MakeTexCode(filename).scale(0.6)
        self.play(code.animate.to_edge(LEFT))
        self.next_slide()

        run_latexmk = MakeCommandLine(
            "latexmk -pdf assets/tex/cross-ref-eqs.tex").scale(0.6).to_corner(DOWN + LEFT)
        self.play(Create(run_latexmk))
        self.next_slide()

        result = ImageMobject("assets/images/cross-ref-eqs.png")
        self.play(result.animate.to_edge(RIGHT))

        self.next_slide()
        self.play(Indicate(code.submobjects[2][4][17:17 + 18]))
        self.play(Indicate(code.submobjects[2][11][10:26]))

        self.next_slide()
        self.play(Indicate(code.submobjects[2][7][17:17 + 30]))
        self.play(Indicate(code.submobjects[2][12][17:17 + 28]))


class IntroductionToTikz(Slide):
    def construct(self):

        title = Text("Introduction to TikZ")
        self.play(Create(title))
        self.next_slide()

        self.play(title.animate.to_corner(UP + LEFT))
        self.next_slide()

        what_is_tikz = Text("What is TikZ?").scale(0.8)
        self.play(Create(what_is_tikz))
        self.next_slide()

        self.play(what_is_tikz.animate.next_to(title, DOWN).shift(1.4 * LEFT))
        self.next_slide()
        answers = VGroup(
            Text(
                """1. A LaTeX package for graphics language
with a manual of over a thousand pages."""),
            Text(
                """2. A number of slowly-paced tutorials that
will teach you almost all you should know about TikZ.""")
        ).scale(0.6).align_to(LEFT)
        answers.arrange(DOWN, center=False, aligned_edge=LEFT)

        self.play(Create(answers.next_to(what_is_tikz, DOWN).shift(4 * RIGHT)))
        self.next_slide()

        why_use_tikz = Text("Why use TikZ?").next_to(
            answers, DOWN * 2 + LEFT * 2).scale(0.8)
        self.play(Create(why_use_tikz))

        self.play(why_use_tikz.animate.next_to(
            answers, DOWN).shift(4 * LEFT))
        self.next_slide()

        answers = VGroup(
            Text(
                """1. When you use TikZ you “program” your graphics,
just as you “program” your document when you use LaTeX."""),
            Text(
                """2. It allows quick creation of simple graphics,
provides precise positioning, allows the use of macros, etc, etc.""")
        ).scale(0.6).align_to(LEFT)
        answers.arrange(DOWN, center=False, aligned_edge=LEFT)
        self.play(Create(answers.next_to(why_use_tikz, DOWN).shift(5 * RIGHT)))

        self.next_slide()
        self.clear()

        install_tikz = MakeCommandLine(
            "sudo apt install -y texlive-latex-base")
        install_tikz.add(
            Text("Install TikZ and necessities on Ubuntu:").scale(0.5).next_to(
                install_tikz, UP))
        install_tikz.add(
            Text("On macOS and on Windows through their packet managers.").scale(0.5).next_to(
                install_tikz, DOWN))
        self.play(Create(install_tikz))


class EuclidsElements(Slide):
    def construct(self):
        title = Text("Euclid's elements. Book I, Proposition I.")
        self.play(Write(title))
        self.next_slide()
        self.clear()

        code1 = MakeTexCode(
            "assets/tex/tikz-euclid-line.tex").scale(0.75).to_corner(UP + LEFT)
        obj1 = ImageMobject(
            "assets/images/tikz-euclid-line.png").shift(2 * (DOWN + RIGHT))
        self.play(Create(code1))
        self.next_slide()
        self.play(FadeIn(obj1))
        self.next_slide()

        code2 = MakeTexCode(
            "assets/tex/tikz-euclid-one-circle.tex").scale(0.75).to_corner(UP + LEFT)
        obj2 = ImageMobject(
            "assets/images/tikz-euclid-one-circle.png").scale(0.6).shift(2 * (DOWN + RIGHT))
        self.play(FadeOut(obj1), ReplacementTransform(code1, code2))
        self.next_slide()
        self.play(FadeIn(obj2))
        self.next_slide()

        code3 = MakeTexCode(
            "assets/tex/tikz-euclid-two-circles.tex").scale(0.75).to_corner(UP + LEFT)
        obj3 = ImageMobject(
            "assets/images/tikz-euclid-two-circles.png").scale(0.6).shift(2 * (DOWN + RIGHT))
        self.play(FadeOut(obj2), ReplacementTransform(code2, code3))
        self.next_slide()
        self.play(FadeIn(obj3))
        self.next_slide()

        code4 = MakeTexCode(
            "assets/tex/tikz-euclid-intersection.tex").scale(0.75).to_corner(UP + LEFT)
        obj4 = ImageMobject(
            "assets/images/tikz-euclid-intersection.png").scale(0.6).shift(2 * (DOWN + RIGHT))
        self.play(FadeOut(obj3), ReplacementTransform(code3, code4))
        self.next_slide()
        self.play(FadeIn(obj4))
        self.next_slide()
        self.clear()

        self.play(Write(Text("And much more...")))
        self.next_slide()
        self.clear()

        self.play(
            FadeIn(MakeTexCode("assets/tex/tikz-euclid.tex").scale(0.5).to_edge(LEFT)))
        self.next_slide()
        self.play(
            FadeIn(ImageMobject("assets/images/tikz-euclid.png").scale(0.6).shift(RIGHT)))
        self.next_slide()


class InstallGnuplot(Slide):
    def construct(self):
        title = Text("Plotting graphics using TikZ.")
        self.play(Write(title))
        self.next_slide()
        self.clear()

        self.play(Write(Text("Install gnuplot").to_corner(UP + LEFT)))

        steps = Group(
            Text("Windows using Chocolatey").shift(LEFT),
            MakeCommandLine("choco install gnuplot"),
            Text("Ubuntu"),
            MakeCommandLine("sudo apt install gnuplot"),
            Text("MacOS"),
            MakeCommandLine("brew install gnuplot")
        ).align_to(LEFT).scale(0.7)

        steps.arrange(DOWN, center=True)
        self.play(FadeIn(steps))
        self.next_slide()
        self.clear()


class ChebyshevPolynomials(Slide):
    def construct(self):
        title = Text("Chebyshev Polynomials")
        self.play(Write(title))
        self.next_slide()

        self.play(title.animate.to_corner(UP + LEFT))
        init_trigo = MathTex("T_n", "(", "\\cos", "\\theta", ")",
                             "=", "\\cos", "(", "n", "\\theta", ")")
        self.play(Create(init_trigo))
        self.next_slide()

        refined_trigo = MathTex("T_n", "(", "x", ")",
                                "=", "\\cos", "(", "n", "\\arccos", "x", ")", ",\\ |x|\\leq 1")
        self.play(TransformMatchingTex(init_trigo, refined_trigo))
        self.next_slide()

        self.play(refined_trigo.animate.next_to(title, DOWN))

        code = MakeTexCode("assets/tex/chebyshev-trigo.tex").scale(0.9)
        self.play(Create(code.to_edge(LEFT)))
        self.next_slide()

        pic = ImageMobject("assets/images/chebyshev-trigo.png").scale(0.6)
        self.play(FadeIn(pic.next_to(code, RIGHT)))
        self.next_slide()

        self.remove(code)
        self.remove(refined_trigo)
        self.remove(pic)

        poly_def = MathTex(
            "T_{n+1}(x)=2xT_n(x)-T_{n-1}(x),\\ T_0(x)=1,\\ T_1(x)=x")
        self.play(Create(poly_def))
        self.next_slide()

        self.play(poly_def.animate.shift(2 * UP))
        self.next_slide()

        poly_sol = MathTex(
            "T_n(x)=\\frac{(x+\\sqrt{x^2 - 1})^n + (x-\\sqrt{x^2-1})^n}{2},\\ x\\in\\mathbf{R}")
        self.play(Create(poly_sol))
        self.next_slide()

        self.play(FadeOut(poly_def, shift=UP))
        self.play(poly_sol.animate.shift(2 * UP))
        self.next_slide()

        code = MakeTexCode("assets/tex/chebyshev-poly.tex").scale(0.9)
        self.play(Create(code.next_to(poly_sol, DOWN).to_edge(LEFT)))
        self.next_slide()

        pic = ImageMobject("assets/images/chebyshev-poly.png").scale(0.4)
        self.play(FadeIn(pic.to_corner((DOWN + RIGHT))))
        self.next_slide()


class Plot3DGraphics(Slide):
    def construct(self):
        title = Text("Three Dimensional Drawing.")
        self.play(Write(title))
        self.next_slide()
        self.clear()

        code1 = MakeTexCode(
            "assets/tex/tikz-3d-graphic.tex").scale(0.4).to_edge(LEFT)
        obj1 = ImageMobject(
            "assets/images/tikz-3d-graphic.png").scale(0.35).to_corner(DOWN + RIGHT)
        self.play(Create(code1))
        self.next_slide()
        self.play(FadeIn(obj1))
        self.next_slide()


class Ending(Slide):
    def construct(self):
        self.next_slide()
        title = Text("Thank you for your attention!")
        self.play(Write(title))
        self.next_slide()