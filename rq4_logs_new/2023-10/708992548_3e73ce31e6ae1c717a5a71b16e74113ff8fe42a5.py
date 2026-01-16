def templates2regex(template):
   regex = template
   regex = regex.replace("\\", "\\\\")
   regex = regex.replace(".", "\.")
   regex = regex.replace("*", "\*")

   regex = regex.replace("<\*>", ".*")

   regex = regex.replace("(", "\(")
   regex = regex.replace(")", "\)")

   regex = regex.replace("[","\[")
   regex = regex.replace("]","\]")

   regex = regex.replace("|","\|")

   regex = regex.replace("+", "\+")
   regex = regex.replace("?", "\?")
   regex = regex.replace("$", "\$")
   regex = regex.replace("@", "\@")
   regex = regex.replace("^", "\^")

   regex = regex.replace(".*", "(.*)")

   #regex = regex.replace(":", "\:")
   #regex = regex.replace("\"", "\\\"")

   regex = '^' + regex + '$'

   return regex