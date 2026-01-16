from pick_word import *

object = PairedType('source_words/object.txt')
object_general = OneType('source_words/object_general.txt')
subject = PairedType('source_words/subject.txt')
secondary = OneType('source_words/secondary.txt')
researcher_action = PairedType('source_words/researcher_action.txt')
subject_action_intrans = PairedType('source_words/subject_action_intrans.txt')
subject_action_trans = PairedType('source_words/subject_action_trans.txt')
researcher = GenderedType('source_words/researcher.txt')
person_type = PairedType('source_words/person_type.txt')
number = NumberType()
adjective = OneType('source_words/adjective.txt')
field = OneType('source_words/field.txt')
research_type = PairedType('source_words/research_type.txt')
researcher_type = PairedType('source_words/researcher_type.txt')

def create_bait():
    i = random.randint(0, 37)
    #i = 25
    print i

    s = ""

    if i == 0:
        s = researcher_type.pick_plural() + " Try to Make " + pick_plural_or_general(object, object_general) + " for " + \
            subject.pick_plural() + ". " + secondary.pick()
    elif i == 1:
        s = "This " + subject.pick_single() + " Was Forced to " + subject_action_intrans.pick_single() + \
            " Because It Didn't Have " + pick_plural_or_general(object, object_general) + ". " + secondary.pick()
    elif i == 2:
        s = "This " + research_type.pick_single() + " Will Prove You've Been " + researcher_action.pick_plural() + " " + \
            pick_plural_or_general(object, object_general) + " Wrong Your Whole Life."
    elif i == 3:
        p = person_type.pick_single()
        s = "Was " + researcher.pick()[0] + " " + a_an(p) + " " + p + "?"
    elif i == 4:
        s = format_num(number.pick()) + " " + subject.pick_plural() + " That " + subject_action_intrans.pick_single() + \
            " Like " + object.pick_plural() + "."
    elif i == 5:
        s = format_num(number.pick()) + " " + object.pick_plural() + " That " + adjective.pick() + " " + subject.pick_plural() + \
            " Won't " + subject_action_trans.pick_single() + "."
    elif i == 6:
        s = format_num(number.pick()) + " " + object.pick_plural() + " That Can't Be Explained by " + \
            subject_action_intrans.pick_plural() + "."
    elif i == 7:
        p = researcher_type.pick_single()
        s = subject.pick_single()
        s = a_an(p).capitalize() + " " + p + " Tried to " + researcher_action.pick_single() + " the " + \
            object.pick_single() + " of " + a_an(s) + " " + s + ". " + secondary.pick()
    elif i == 8:
        s = format_num(number.pick()) + " " + adjective.pick() + " " + object.pick_plural() + " That Can Be Verified by " + \
            researcher_action.pick_plural() + " " + subject.pick_plural() + "."
    elif i == 9:
        r = researcher_type.pick_single()
        o = object.pick_single()
        s = a_an(r).capitalize() + " " + r + " Tries to " + researcher_action.pick_single() + " " + a_an(o) + " " + o + \
            ". " + secondary.pick()
    elif i == 10:
        s = format_num(number.pick()) + " " + object.pick_plural() + " That " + subject.pick_plural() + " " + \
            subject_action_trans.pick_single() + "."
    elif i == 11:
        s = "Was " + researcher.pick()[0] + " Wrong About " + pick_plural_or_general(object, object_general) + "?"
    elif i == 12:
        s = "Is " + field.pick() + " Wrong About " + pick_plural_or_general(object, object_general) + "?"
    elif i == 13:
        s = "This " + research_type.pick_single() + " About " + pick_plural_or_general(object, object_general) + \
            " Says That " + researcher_type.pick_plural() + " Should " + researcher_action.pick_single() + " " + \
            subject.pick_plural() + "."
    elif i == 14:
        s = "Did {0} {1} {2}?".format
        s = "Did " + researcher.pick()[0] + " " + researcher_action.pick_single() + " " + subject.pick_plural() + "?"
    elif i == 15:
        s = "Who Knew " + adjective.pick() + " " + object.pick_plural() + " Could Be So " + adjective.pick() + "?"
    elif i == 16:
        s = "Do " + adjective.pick() + " " + object.pick_plural() + " Really Cause " + adjective.pick() + " " + \
            object.pick_plural() + "?"
    elif i == 17:
        s = researcher_type.pick_plural() + " Find New " + adjective.pick() + " " + object.pick_single() + \
            " That Could Let " + pick_plural_or_general(object, object_general) + " " + \
            subject_action_intrans.pick_single() + "!"
    elif i == 18:
        s = "The Truth About {0} {1}.".format(
            adjective.pick(), pick_plural_or_general(object, object_general))
    elif i == 19:
        s = "Have {0} Been Lying About {1} {2}?".format(
            researcher_type.pick_plural(), adjective.pick(), pick_plural_or_general(object, object_general))
    elif i == 20:
        s = "Meet the Bad-Ass {} Who Changed the Way {} {} {}.".format(
            researcher_type.pick_single(), subject.pick_plural(), subject_action_trans.pick_single(),
            pick_plural_or_general(object, object_general))
    elif i == 21:
        s = "Ever Wonder Why {} {}? These {} Facts Will Blow Your Mind!".format(
            subject.pick_plural(), subject_action_intrans.pick_single(), number.pick())
    elif i == 22:
        x1 = researcher.pick()
        s = "No One Believed {} Until {} Discovered the Truth About {}. Now Other {} Hate {}!".format(
            x1[0], get_pronoun(x1[1]), pick_plural_or_general(object, object_general), researcher_type.pick_plural(),
            get_pronoun_acc(x1[1]))
    elif i == 23:
        s = "You Thought All {} Have the Same {}, Right? Here are {} Reasons Why You're Wrong.".format(
            subject.pick_plural(), pick_plural_or_general(object, object_general), number.pick())
    elif i == 24:
        s = "These {} Needed to Get Their Message Across. How They Did It Amazed Me. (Hint: It May Have Involved {}.)".format(
            subject.pick_plural(), pick_plural_or_general(object, object_general))
    elif i == 25:
        x1 = number.pick()
        x2 = number.pick()
        n1 = min([x1, x2])
        n2 = max([x1, x2])
        s = "Think You Know How {} Work? Listen to This {}. On Page {} He'll Make You Think. On Page {} He'll Blow Your Mind.".format(
            object.pick_plural(), researcher_type.pick_single(), format_num(n1), format_num(n2))
    elif i == 26:
        s = "You Won't Believe What's Causing the Spread of {} {}! The Shocking Truth That {} Don't Want You to Hear.".format(
            adjective.pick(), pick_plural_or_general(object, object_general), researcher_type.pick_plural())
    elif i == 27:
        s = "Think {} Like to {}? Think Again.".format(
            subject.pick_plural(), subject_action_intrans.pick_single())
    elif i == 28:
        s = "These {} Were Forced to {} {} Because of {}. {}".format(
            subject.pick_plural(), subject_action_trans.pick_single(), pick_plural_or_general(object, object_general), object.pick_plural(), secondary.pick())
    elif i == 29:
        s = "{} {} Warned Us to Look for {} {}. {}".format(
            object.pick_single(), researcher_type.pick_plural(), adjective.pick(), pick_plural_or_general(object, object_general), secondary.pick())
    elif i == 30:
        s = "{} {} Guaranteed to Make {} {}.".format(
            number.pick(), object.pick_plural(), subject.pick_plural(), subject_action_intrans.pick_single())
    elif i == 31:
        s = "The New {} {} That Every {} Is Talking About.".format(
            adjective.pick(), object.pick_single(), researcher_type.pick_single())
    elif i == 32:
        s = "{} Warned Us About {}, But Look What's Happening Now.".format(
            researcher.pick()[0], pick_plural_or_general(object, object_general))
    elif i == 33:
        s = "Read This {} to Discover the True Meaning of {}.".format(
            research_type.pick_single(), object.pick_plural())
    elif i == 34:
        s = "Why All {} {}. {}".format(
            subject.pick_plural(), subject_action_intrans.pick_single(), secondary.pick())
    elif i == 35:
        s = "The Real {} Secrets of {}.".format(
            adjective.pick(), subject.pick_plural())
    elif i == 36:
        s = "These Facts About {} Will Change the Way You Look at {} Forever.".format(
            pick_plural_or_general(object, object_general), subject.pick_plural())
    elif i == 37:
        s = "{} Ways That {} {}.".format(
            number.pick(), subject.pick_plural(), subject_action_intrans.pick_single())

    #lst = [word[0].upper() + word[1:] for word in s.split()]
    #s = " ".join(lst)
    return s

print create_bait()