import os
import webbrowser


paper_lower_hsv = [31, 184, 184]
paper_upper_hsv = [47, 255, 237]

header_lower_hsv_values = [103, 37, 105]
header_upper_hsv_values = [138, 122, 199]
roi = [[(281, 360), (484, 447)], [(507, 359), (706, 451)], [(723, 363), (923, 450)], [(944, 356), (1135, 446)], [(1154, 358), (1345, 445)], [(283, 468), (480, 566)],
       [(501, 464), (703, 562)], [(722, 467), (922, 565)], [(942, 468), (1137, 563)], [(1152, 466), (1351, 558)], [(281, 575), (470, 676)], [(501, 580), (699, 674)],
       [(722, 580), (910, 680)], [(935, 579), (1137, 678)], [(1149, 571), (1346, 675)], [(282, 692), (476, 795)], [(498, 690), (692, 799)], [(716, 688), (915, 794)],
       [(930, 688), (1126, 792)], [(1150, 692), (1338, 788)], [(273, 807), (465, 894)], [(489, 810), (689, 900)], [(709, 809), (913, 902)], [(931, 811), (1126, 905)],
       [(1147, 810), (1340, 907)]]

command = {
                "application" : os.system,
                "url":webbrowser.open
            }
# command["app"]("gnome-calculator")

# application(name)
# url('http://net-informations.com', new=2)

application_names = [["gmail", "url", "https://mail.google.com/"],
                     ["uohydemail", "", ""],
                     ["wikipedia", "url", "https://en.wikipedia.org/wiki/Main_Page"],
                     ["youtube", "url", "https://www.youtube.com/"],
                     ["writer", "", ""],
                     ["firefox", "application", "firefox"],
                     ["google", "application", "google-chrome"],
                     ["myhomepage", "url", "https://www.linkedin.com/feed/"],
                     ["empty","",""],
                     ["spredsheet", "application", "libreoffice -o /home/dpavan.reddy/new.odt"],
                     ["ciptk", "", ""],
                     ["gimp","", ""],
                     ["draw", "application", "libreoffice -o /home/dpavan.reddy/Documents/Untitled.odg"],
                     ["xfig", "url", "https://www.xfig.org/"],
                     ["impress", "url", "https://www.openoffice.org/product/impress.html"],
                     ["eog", "url", "https://www.eogresources.com/"],
                     ["tangrams", "url", "https://mathigon.org/tangram"],
                     ["spirograph", "url", "https://seedcode.com/SpirographN/sgn.html"],
                     ["empty", "", ""],
                     ["calculatore", "application", "gnome-calculator"],
                     ["terminal", "application", "gnome-terminal"],
                     ["latexlayout", "url", "https://latexbase.com/d/f2536d45-137b-49d7-8fdd-8e35a64db077"],
                     ["empty", "", ""],
                     ["empty", "", ""],
                     ["exit", "exit", "exit"]
                     ]