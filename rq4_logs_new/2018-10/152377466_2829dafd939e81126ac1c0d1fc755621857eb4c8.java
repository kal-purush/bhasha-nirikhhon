/***************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    error.java                                         :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: thmotaun <marvin@42.fr>                    +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2017/10/29 12:36:06 by thmotaun          #+#    #+#              #
#    Updated: 2017/10/29 12:36:09 by thmotaun         ###   ########.fr        #
#                                                                              #
# *****************************************************************************/

import java.util.*;

public class error
{
		public error()
		{
			// empty constructor
		}


		private boolean isOperator(char c)
		{
			if (c == '+' || c == '|' || c == '>' || c == '^')
			{
				return true;
			}
			return false;
		}

		private boolean isfactelem(char c)
		{
			if ((c >= 'A' && c <= 'Z') || c == '!' || c == '=' || c == '(' || c == ')' || c == '?')
			{
				return true;
			}
			return false;
		}

		private boolean goodRule(String rule)
		{
			if (!isfactelem(rule.charAt(0)))
			{
				return false;
			}
			for (int i = 0; i < (int)rule.length(); i++)
			{
				if (isOperator(rule.charAt(i)) && !isfactelem(rule.charAt(++i)))
				{
					return false;
				}
			}
			return true;
		}

		public final boolean errorCheck(ArrayList<String> lines)
		{
			int inifacts = 0;
			int queries = 0;
			int errors = 0;
			int i = 0;

			while (i < (int)lines.size())
			{
				if (lines.get(i).charAt(0) == '=')
				{
					inifacts++;
				}
				if (lines.get(i).charAt(0) == '?')
				{
					queries++;
				}
				if (goodRule(lines.get(i)) == false)
				{
					errors++;
					System.out.println((char)27 + "[31m" + "Error: Invalid rule found at Rule: " + i + "!");
				}
				i++;
			}
			if (queries > 1)
			{
				System.out.println((char)27 + "[36m" + "Warning: More than 1 query statement found!");
			}
			if (inifacts > 1)
			{
				System.out.println((char)27 + "[31m" + "Error: More than 1 \'Initial fact\' statement found!");
				errors++;
			}

			if (queries == 0)
			{
				System.out.println((char)27 + "[31m" + "Error: No Queries found!");
				errors++;
			}
			if (inifacts == 0)
			{
				System.out.println((char)27 + "[31m" + "Error: No Initial facts set!");
				errors++;
			}

			if (errors > 0)
			{
				return true;
			}
			return false;
		}
}