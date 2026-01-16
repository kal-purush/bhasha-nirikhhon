package me.doubledutch.options;

//    Copyright [2015] [DoubleDutch]
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

import java.util.*;
import java.util.regex.*;
import java.lang.reflect.*;

public class DDOptions{
	private static Pattern optionPattern=Pattern.compile("([^\\:]+)(\\:([^\\-]+)(\\-\\>(.+))?)?");

	/**
	 * Splits the option configuration format into its three possible parts.
	 *
	 * @param option the option configuration to be parsed
	 * @return The three possible components of an option configuration or null if the configuration could not be parsed.
	 */
	protected static String[] splitOption(String option){
		String[] result=new String[3];
		Matcher m=optionPattern.matcher(option);
		if(m.matches()){
			result[0]=m.group(1);
			result[1]=m.group(3);
			result[2]=m.group(5);
			// If no type is given, boolean is used
			if(result[1]==null)result[1]="boolean";
			// If no target field is given, the option name is used
			if(result[2]==null)result[2]=result[0];
			return result;
		}
		return null;
	}

	/**
	 * Iterates through a set of options to find the one matching the given key - if it exists.
	 *
	 * @param key The name of the option to find
	 * @param optionSet The set of option configuration strings
	 * @return The requested option configuration string or null if it could not be found
	 */
	protected static String getOption(String key, String[] optionSet){
		for(String option:optionSet){
			// TODO: perhaps go back to simpler parsing or cache regexp results?
			String[] parsed=splitOption(option);
			if(parsed!=null){
				if(parsed[0].equals(key)){
					return option;
				}
			}
		}
		return null;
	}

	/**
	 * Determines wether or not an option takes an argument. Currently, everything but booleans require an argument.
	 * The output is undefined (read, it will also return true) if the option configuration could not be parsed.
	 *
	 * @param option the option configuration to check
	 * @return true if the given option takes an argument.
	 */
	protected static boolean needsArgument(String option){
		String[] parsed=splitOption(option);
		if(parsed!=null){
			if(parsed[1].equals("boolean")){
				return false;
			}
		}
		return true;
	}

	/**
	 * Attempts to use reflection to get the Field on an object matching the given name.
	 * If the field is found, it will be set to be accessible even if it's private or protected.
	 *
	 * @param target the object whose class should be searched for the given field.
	 * @param field the name of the field to locate
	 * @return the field object if it exists, null otherwise
	 */
	private static Field getOptionField(Object target,String field){
		try{
			Field f=target.getClass().getDeclaredField(field);
			if(f!=null){
				f.setAccessible(true);
				return f;
			}
		}catch(NoSuchFieldException nfe){}
		return null;
	}

	/**
	 * Attempts to use reflection to get the method on an object matching the given name and type.
	 * If the method is found, it will be set to be accessible even if it's private or protected.
	 *
	 * @param target the object whose class should be searched for the given method.
	 * @param methodName the name of the method to locate
	 * @param type the class type of the method's single argument
	 * @return the method object if it exists, null otherwise
	 */
	private static Method getOptionMethod(Object target,String methodName,Class type){
		try{
			Method m=target.getClass().getDeclaredMethod(methodName,new Class[]{type});
			m.setAccessible(true);
			return m;
		}catch(NoSuchMethodException nsme){}
		return null;
	}

	/**
	 * Uses getOptionMethod to search for methods either matching the name, or matching the name with a "set" in front of it.
	 * This having field set to user will search for methods called user and methods called setUser.
	 */
	private static Method searchForOptionMethod(Object target,String field,Class type){
		Method m=getOptionMethod(target,field,type);
		if(m!=null)return m;
		String searchName="set"+field.substring(0,1).toUpperCase()+field.substring(1);
		m=getOptionMethod(target,searchName,type);
		return m;
	}


	/**
	 * Attempts to parse and set an option on the target object.
	 *
	 * @return Null if it was successful or an error message if the option could not be set.
	 */
	private static String setOption(Object target,String arg,String option){
		String[] split=splitOption(option);
		String key=split[0];
		String field=split[2];
		String type=split[1];
		Field objField=getOptionField(target,field);
		try{
			if(type.equals("boolean")){
				if(objField!=null){
					objField.setBoolean(target,true);
				}else{
					Method objMethod=searchForOptionMethod(target,field,boolean.class);
					objMethod.invoke(target,true);
				}
			}else if(type.equals("string")){
				if(objField!=null){
					objField.set(target,arg);
				}else{
					Method objMethod=searchForOptionMethod(target,field,String.class);
					objMethod.invoke(target,arg);
				}
			}else if(type.equals("integer")){
				int i=Integer.parseInt(arg);
				if(objField!=null){
					objField.setInt(target,i);
				}else{
					Method objMethod=searchForOptionMethod(target,field,int.class);
					objMethod.invoke(target,i);
				}
			}else if(type.equals("float")){
				Float f=Float.parseFloat(arg);
				if(objField!=null){
					objField.setFloat(target,f);
				}else{
					Method objMethod=searchForOptionMethod(target,field,float.class);
					objMethod.invoke(target,f);
				}
			}else if(type.equals("timestamp")){
				// TODO: Add automatic fill in of values so a time today can simply be specified as hh:mm
				long timestamp=javax.xml.bind.DatatypeConverter.parseDateTime(arg).getTimeInMillis();
				if(objField!=null){
					objField.setLong(target,timestamp);
				}else{
					Method objMethod=searchForOptionMethod(target,field,long.class);
					objMethod.invoke(target,timestamp);
				}
			}else{
				// TODO: perhaps throw exception instead?
			 	return "The configuration for -"+key+" is invalid - this is an application error";
			}
			return null;
		}catch(NumberFormatException nfe){
					return "The argument to -"+key+" is not valid a valid number";
		}catch(IllegalArgumentException iae){
			// iae.printStackTrace();
			return "The argument to -"+key+" is not a valid ISO 8601 timestamp";
		}catch(Exception e){
			e.printStackTrace();
		}
		// TODO: perhaps throw exception instead?
		return key+" could not be set - this is an application error";
	}

	/**
	 * Parse a set of options matchning the given configuration and attempt to set it on the given target object.
	 * 
	 * The format of a single option configuration is as follows:
	 * &lt;option&gt; = &lt;identifier&gt; (':' &lt;type&gt; ('->' &lt;mappedIdentifier&gt;)?)?
	 *
	 * If no type is specified, boolean is assumed. If no mapped identifier is specified, the base identifier is used.
	 * Here are some samples of valid option configurations:
	 * <ul>
	 *   <li>help
	 *   <li>count:integer
	 *   <li>threads:integer->setThreadCount
	 *   <li>log:string->logFilename
	 * </ul> 
	 *
	 */
	public static List<String> setOptions(Object target,String[] data,String... optionSet){
		List<String> errors=new LinkedList<String>();

		// Run through all arguments
		for(int index=0;index<data.length;index++){
			String value=data[index];
			if(value.startsWith("-")){
				String key=value.substring(1);
				if(key.startsWith("-"))key=key.substring(1);
				String option=getOption(key,optionSet);
				if(option!=null){
					if(needsArgument(option)){
						if(index+1<data.length){
							index+=1;
							String arg=data[index];
							String result=setOption(target,arg,option);
							if(result!=null){
								errors.add(result);
							}
						}else{
							errors.add("Missing argument for option "+value);
						}
					}else{
						String result=setOption(target,null,option);
						if(result!=null){
							errors.add(result);
						}
					}
				}
			}
		}
		return errors;
	}
}