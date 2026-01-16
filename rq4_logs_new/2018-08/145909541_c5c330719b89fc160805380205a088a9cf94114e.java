import java.io.*;
import java.util.*;








/**
 * 
 */

/**
 * @author shawon
 *
 */


public class DTree {
	
	

	/**
	 * @param args
	 * @throws IOException 
	 */
	int ROW_ATTRIBUTE=3;
	int COL_ATTRIBUTE=101;
	double TOTAL_YES=0;
	double TOTAL_NO=0;
	double TOTAL_ATTRIBUTE=0;
	double IG=0;
	/*This is a two dimensional array
	 * where row holds the attribute of the outlook
	 * and the column holds the attribute of the temperature*/
	int [][]count_attribute=new int[ROW_ATTRIBUTE][COL_ATTRIBUTE];	 	
	
	public void readDataFile() throws IOException
	{
		
		
		 String s1;
		 String s2;
		 String s3;
		 int pos, i, j;
		 int temp;
         
		  // set up the buffered reader to read from the keyboard
		 BufferedReader br = new BufferedReader (new FileReader ("C:/Users/shawon/workspace/test/src/weather.arff"));
         s1="";
         s2="";
         pos=0;
		  while((s1=br.readLine())!=null)
		  {
		    //System.out.println(s1);
			if(!s1.contains("@")) 
			{
		      s2 += s1+"\n";
		      pos++;
			}
		  }
         
		  //System.out.println ("The line is " + s2);
		  //System.out.println ("The line has " + s1.length() + " characters");

		  System.out.println ();
		  //System.out.println ("Breaking the line into tokens we get:");

		  int neg = 0;
		  //Data[] data = new Data[n];
		  StringTokenizer st = new StringTokenizer (s2);
         
		  while (st.hasMoreTokens()) 
		  {
		      s2 = st.nextToken();
		      s1 = s2;
		      StringTokenizer st2 = new StringTokenizer(s1,",");
		      //while(st2.hasMoreTokens())
		      //{
		    	s3 = st2.nextToken();
		    	if(s3.startsWith("overcast"))
		    	{
		    		temp = Integer.parseInt(st2.nextToken());
		    		/*System.out.println("Temp "+temp);*/
		    		s3=st2.nextToken();
		    		if(s3.equalsIgnoreCase("yes"))
		    			count_attribute[0][temp]+= 1;
		    		else if(s3.equalsIgnoreCase("no"))
		    			count_attribute[0][temp]-=1;
		    		
		    	}
		    	else if(s3.startsWith("rainy"))
		    	{
		    		
		    		temp = Integer.parseInt(st2.nextToken());
		    		/*System.out.println("Temp "+temp);*/
		    		s3=st2.nextToken();
		    		if(s3.equalsIgnoreCase("yes"))
		    			count_attribute[1][temp]+=1;
		    		else if(s3.equalsIgnoreCase("no"))
		    			count_attribute[1][temp]-=1;
		    		
		    		
		    	}
		    	else if(s3.startsWith("sunny"))
		    	{		    		
		    		
		    		temp = Integer.parseInt(st2.nextToken());
		    		//System.out.println("sunny Temp "+temp);
		    		s3=st2.nextToken();
		    		if(s3.equalsIgnoreCase("yes"))
		    			count_attribute[2][temp]+=1;
		    		else if(s3.equalsIgnoreCase("no"))
		    			count_attribute[2][temp]-=1;
		    	}
		    		
		      //}
		      
		      /*numTokens++;
		      System.out.println ("    Token " + numTokens + " is: " + s2);*/
		      
		      
		}
		
		for(i=0; i<3; i++)
		{  
			pos=0;
			neg=0;	
		   for(j=60; j<=100; j++)
		   {
			   if(count_attribute[i][j]>0)
				   pos+=count_attribute[i][j];
			   else if(count_attribute[i][j]<0)
				   neg+=(count_attribute[i][j]*(-1));
			   
		   }
		   //System.out.println(i +":->"+" Yes " + pos + " No "+ neg);
		   TOTAL_YES += pos;
		   TOTAL_NO += neg;
		}
		
		TOTAL_ATTRIBUTE = TOTAL_YES+TOTAL_NO;
		//System.out.println(" Total yes: "+ TOTAL_YES+" Total no: "+TOTAL_NO+" Total num:"+ TOTAL_ATTRIBUTE);
		
		IG = -((TOTAL_YES/TOTAL_ATTRIBUTE)*(Math.log10(TOTAL_YES/TOTAL_ATTRIBUTE)/Math.log10(2))
		      +((TOTAL_NO/TOTAL_ATTRIBUTE)*(Math.log10(TOTAL_NO/TOTAL_ATTRIBUTE)/Math.log10(2.0))));
		System.out.println("Total entropy:"+IG);
		
	}
	
	/*Calculates information gain and returns the index of the highest 
	 * ig of the attribute*/
	public int calculateTotalInformationGain()
	{
		double igo, ign, temp;
		/*Calculates ig of the outlook at first*/
		int pos, neg, j, i, k, index, index2;
		double numerator, denominator, total, total_entropy,IGO, IGT;
		igo = 0;
		temp = -65535;
		index = -1;
		IGO = 0;
		System.out.println("Information gain (IG) calculation of the outlook attribute");
		total_entropy=0;
		
		for(i=0; i<ROW_ATTRIBUTE; i++)
		{  
			pos=0;
			neg=0;
			
			total = 0;
		    for(j=60; j<COL_ATTRIBUTE; j++)
		    { 
			   if(count_attribute[i][j]>0)
				   pos+=count_attribute[i][j];
			   else if(count_attribute[i][j]<0)
				   neg+=(count_attribute[i][j]*(-1));
			   
		    }
		    if(pos == 0 || neg == 0)
		    	igo =  0;
		    else if (pos == neg)
		    	igo =  1;
		    	//igo = IG;
		    else
		    {
		      numerator = pos;
		      denominator = neg;
		      total = numerator + denominator;
		      igo =  - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
			    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
		      
		    }
		    if(i==0)
		    	System.out.println("overcast entropy: "+igo);
		    else if(i==1)
		    	System.out.println("rainy entropy: "+igo);
		    else if(i==2)
		    	System.out.println("sunny entropy: "+igo);
		    total = pos+neg;
		    total_entropy += (total/TOTAL_ATTRIBUTE)*igo;
		    //System.out.println(total_entropy+"  "+total+"  "+TOTAL_ATTRIBUTE);
		}		
		//System.out.println("Total entropy"+ total_entropy);
		IGO = IG - total_entropy; 
		System.out.println("Information gain of the overlook attribute IG:" + IGO);
		
		
		k=65;
		ign=0;
		temp = -65535;
		index2 = -1;
		IGT = 0;
		System.out.println("Information gain (IG)calculation of the temperature attribute");
		while(k<=95)
		{
		    pos=0;
		    neg=0;
		    total_entropy=0;
			total = 0;
		    for(i=0; i<ROW_ATTRIBUTE; i++)
		    {
			
			    /*Check to the left*/
			   for(j=0; j<=k; j++)
			   {
				   if(count_attribute[i][j]>0)
					   pos+=count_attribute[i][j];
				   else if(count_attribute[i][j]<0)
					   neg+=(count_attribute[i][j]*(-1));
				
			   }
		    }
			if(pos == 0 || neg == 0)
		    	  ign = 0;
		    else if (pos == neg)
		    	  ign = 1;
		    	  //ign = IG;
		    else
		    {
		      numerator = pos;
		      denominator = neg;
		      total = numerator + denominator;
		      ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
			    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
		      
		      //System.out.println(k +"<= entropy "+ pos +" "+ neg+" "+ign);
		      
		    }
			

		    System.out.println("entropy of the temperaure  <=  "+k+" is  "+ign);
            total=pos+neg;  
		    total_entropy += (total/TOTAL_ATTRIBUTE)*ign;
		    
			
		    pos=0;
			neg=0;
			for(i=0; i<ROW_ATTRIBUTE; i++)
		    {
			    /*Check to the right*/
			    for(j=k+1; j<COL_ATTRIBUTE; j++)
			    {
				      if(count_attribute[i][j]>0)
					    pos+=count_attribute[i][j];
				      else if(count_attribute[i][j]<0)
					    neg+=(count_attribute[i][j]*(-1));
				
			    }
			
		     }
			 
			if(pos == 0 || neg == 0)
		    	  ign = 0;
		    else if (pos == neg)
		    	  ign = 1;
		    	  //ign = IG;
		    else
		    {
		      numerator = pos;
		      denominator = neg;
		      total = numerator + denominator;
		      ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
			    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
		      
		      //System.out.println(k +">  entropy "+ pos +" "+ neg+" "+ign);
		      //ign =	IG -ign;
		    }

		    System.out.println("entropy of the temperaure > "+k+" is "+ign);
		    total = pos+neg;
		    total_entropy += (total/TOTAL_ATTRIBUTE)*ign;
		    IGT = IG - total_entropy;
		    
		    if(temp < IGT)
		    {
		    	temp = IGT;
		    	index2 = k;
		    }
		  
		  k += 5; /* 5 increment step*/
		}
		
		System.out.println("Best information gain of Temperature: "+index2+" "+temp);
		
		
		if(IGT > IGO)
		{
			System.out.println("Temperature "+index2+" has the largest information gain :"+ IGT);
			return index2;
		}
		else
		{
			
			  System.out.println("Overlook has the largest information gain:"+ IGO);		
			  return 0;
		}
		
		
		
	}
	
	public double calculateOutlookEntropy(int index)
	{
		
		int pos, neg, j;
		double numerator, denominator, total, ign;
		
		pos=0;
		neg=0;
		for(j=60; j<COL_ATTRIBUTE; j++)
		{
			if(count_attribute[index][j]>0)
			    pos+=count_attribute[index][j];
		    else if(count_attribute[index][j]<0)
			    neg+=(count_attribute[index][j]*(-1));
	
		}
		
		if(pos == 0 || neg == 0)
	    	  return 0.0;
	    else if (pos == neg)
	    	  return 1.0;
	    else
	    {
	    	numerator = pos;
		    denominator = neg;
		    total = numerator + denominator;
		    ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
			    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
		    return ign;
	    }
	    
		
		
		
	}
	
	
	public double calculateTotalTempEntropy(int index)
	{
		int pos, neg, i,j;
		double numerator, denominator, total, ign;
		
		if((index+1)%5 == 0)
		{
		   index += 1;
		   pos = 0;
		   neg = 0;
		   /*less than part*/
		   for(i=0; i<ROW_ATTRIBUTE; i++)
		   {
			  for(j=0; j<=index; j++)
			  {
				  
				  if(count_attribute[i][j]>0)
					 pos+=count_attribute[i][j];
				  else if(count_attribute[i][j]<0)
					 neg+=(count_attribute[i][j]*(-1));
				  
			  }
				  
		   }
		   if(pos == 0 || neg == 0)
		    	  return 0.0;
		    else if (pos == neg)
		    	  return 1.0;
		    else
		    {
		    	numerator = pos;
			    denominator = neg;
			    total = numerator + denominator;
			    ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
				    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
			    return ign;
		    }
		   
			
		}
		else
		{
		  /*greater than part*/
			index += 1;
			pos = 0;
			neg = 0;
			   /*less than part*/
			for(i=0; i<ROW_ATTRIBUTE; i++)
			{
				  for(j=index; j<COL_ATTRIBUTE; j++)
				  {
					  
					  if(count_attribute[i][j]>0)
						 pos+=count_attribute[i][j];
					  else if(count_attribute[i][j]<0)
						 neg+=(count_attribute[i][j]*(-1));
					  
				  }
					  
			 }
			 if(pos == 0 || neg == 0)
			    	  return 0.0;
			 else if (pos == neg)
			    	  return 1.0;
			 else
			 {
			    	numerator = pos;
				    denominator = neg;
				    total = numerator + denominator;
				    ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
					    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
				    return ign;
			  }
				
			
			
		}
		
		
		
		
		
		
	}
	
	
	public double calculateTempEntropy(int row, int index)
	{
		int pos, neg, i,j;
		double numerator, denominator, total, ign;
		
		if((index+1)%5 == 0)
		{
		   index += 1;
		   pos = 0;
		   neg = 0;
		   /*less than part*/
		   //for(i=0; i<ROW_ATTRIBUTE; i++)
		   //{
			  for(j=60; j<=index; j++)
			  {
				  
				  if(count_attribute[row][j]>0)
					 pos+=count_attribute[row][j];
				  else if(count_attribute[row][j]<0)
					 neg+=(count_attribute[row][j]*(-1));
				  
			  }
				  
		   //}
		   if(pos == 0 || neg == 0)
		    	  return 0.0;
		    else if (pos == neg)
		    	  return 1.0;
		    else
		    {
		    	numerator = pos;
			    denominator = neg;
			    total = numerator + denominator;
			    ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
				    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
			    return ign;
		    }
		   
			
		}
		else
		{
		  /*greater than part*/
			index += 1;
			pos = 0;
			neg = 0;
			   /*less than part*/
			//for(i=0; i<ROW_ATTRIBUTE; i++)
			//{
				  for(j=index; j<COL_ATTRIBUTE; j++)
				  {
					  
					  if(count_attribute[row][j]>0)
						 pos+=count_attribute[row][j];
					  else if(count_attribute[row][j]<0)
						 neg+=(count_attribute[row][j]*(-1));
					  
				  }
					  
			 //}
			 if(pos == 0 || neg == 0)
			    	  return 0.0;
			 else if (pos == neg)
			    	  return 1.0;
			 else
			 {
			    	numerator = pos;
				    denominator = neg;
				    total = numerator + denominator;
				    ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
					    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
				    return ign;
			  }
				
			
			
		}
		
	   	
	   	
	}
	
	public int findDecision(int row,int index)
	{
	   int i,j,pos,neg;
	   //System.out.println("find decision "+index);
	   if((index+1)%5 == 0)
	   {
		   index += 1;
		   pos=0;
		   neg=0;
		   for(j=60; j<= index; j++)
		   {
			   if(count_attribute[row][j]>0)
				 pos+=count_attribute[row][j];
			   else if(count_attribute[row][j]<0)
				   neg+=(count_attribute[row][j]*(-1));
		   }
		   //System.out.println(row +" less than temp"+ index+" "+pos+" "+neg);
		   if(pos > neg)
			   return 1;
		   else if(pos < neg)
			   return -1;
		   else if(pos==neg)
		   {
			   pos =0;
			   neg=0;
			   for(j=60; j<COL_ATTRIBUTE; j++)
			   {
				   if(count_attribute[row][j]>0)
					 pos+=count_attribute[row][j];
				   else if(count_attribute[row][j]<0)
					   neg+=(count_attribute[row][j]*(-1));
			   }   
			   
			   if(pos > neg)
				   return 1;
			   else if(pos < neg)
				   return -1;
			   else 
				   return 1;
		   }		       		   
	   }
	   else 
	   {
		   index += 1;
		   pos=0;
		   neg=0;
		   
		   for(j=index; j<COL_ATTRIBUTE; j++)
		   {
			   if(count_attribute[row][j]>0)
				 pos+=count_attribute[row][j];
			   else if(count_attribute[row][j]<0)
				   neg+=(count_attribute[row][j]*(-1));
		   }
		   
		   //System.out.println(row +" greater than temp "+index+" "+pos+" "+neg);
		   if(pos>neg)
			   return 1;
		   else if(pos<neg)
			   return -1;
		   else if(pos==neg)
		   {
			   /*Search the whole array to get the decision*/
			   pos =0;
			   neg=0;
			   for(j=60; j<COL_ATTRIBUTE; j++)
			   {
				   if(count_attribute[row][j]>0)
					 pos+=count_attribute[row][j];
				   else if(count_attribute[row][j]<0)
					   neg+=(count_attribute[row][j]*(-1));
			   }   
			   
			   if(pos>neg)
				   return 1;
			   else if(pos<neg)
				   return -1;
			   else 
				   return 1;
		   }
		   
	   }
	   
	   return 0;
	}
	
	public int getBestEntropyForOutlook(int index)
	{
        int i,j, k, neg, pos, index2;
        double numerator, denominator, total, ign, temp, total_entropy, IGT, IG;
        k=65;
        neg=0;
        pos=0;
        temp = -65535;
        index2 = -1;
        total_entropy = 0;
        IGT = 0;
        IG = 0;
        /*calculating information gain of the outlook*/
        for(j=0; j< COL_ATTRIBUTE; j++)
        {
        	if(count_attribute[index][j]>0)
				   pos+=count_attribute[index][j];
			   else if(count_attribute[index][j]<0)
				   neg+=(count_attribute[index][j]*(-1));
        }
        if(pos == 0 || neg == 0)
	    	  IG = 0;
	    else if (pos == neg)
	    	  IG = 1;
	    	  //ign = IG;
	    else
	    {
	      numerator = pos;
	      denominator = neg;
	      total = numerator + denominator;
	      IG = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
		    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
	      
	    }
        
        //System.out.println("Entropy "+index+" "+IG);
        k=65;
		ign=0;
		temp = -65535;
		index2 = -1;
		IGT = 0;
		//System.out.println("Information gain (IG)calculation of the temperature attribute");
		
		while(k<=95)
		{
		    pos=0;
		    neg=0;
		    total_entropy=0;
			total = 0;
		    //for(i=0; i<ROW_ATTRIBUTE; i++)
		    //{
			
			    /*Check to the left*/
			 for(j=0; j<=k; j++)
			 {
				   if(count_attribute[index][j]>0)
					   pos+=count_attribute[index][j];
				   else if(count_attribute[index][j]<0)
					   neg+=(count_attribute[index][j]*(-1));
				
			  }
		    //}
			if(pos == 0 || neg == 0)
		    	  ign = 0;
		    else if (pos == neg)
		    	  ign = 1;
		    	  //ign = IG;
		    else
		    {
		      numerator = pos;
		      denominator = neg;
		      total = numerator + denominator;
		      ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
			    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
		      
		      //System.out.println(k +"<= entropy "+ pos +" "+ neg+" "+ign);
		      
		    }
			
		    //System.out.println("entropy of the temperaure "+k+" <= : "+ign);
			total = pos+neg;
		    total_entropy += (total/TOTAL_ATTRIBUTE)*ign;
		    
			
		    pos=0;
			neg=0;
			//for(i=0; i<ROW_ATTRIBUTE; i++)
		    //{
			    /*Check to the right*/
			    for(j=k+1; j<COL_ATTRIBUTE; j++)
			    {
				      if(count_attribute[index][j]>0)
					    pos+=count_attribute[index][j];
				      else if(count_attribute[index][j]<0)
					    neg+=(count_attribute[index][j]*(-1));
				
			    }
			
		     //}
			 
			if(pos == 0 || neg == 0)
		    	  ign = 0;
		    else if (pos == neg)
		    	  ign = 1;
		    	  //ign = IG;
		    else
		    {
		      numerator = pos;
		      denominator = neg;
		      total = numerator + denominator;
		      ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
			    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
		      
		      //System.out.println(k +">  entropy "+ pos +" "+ neg+" "+ign);
		      //ign =	IG -ign;
		    }

		    //System.out.println("entropy of the temperaure "+k+" >: "+ign);
			total = pos+neg;
		    total_entropy += (total/TOTAL_ATTRIBUTE)*ign;
		    IGT = IG - total_entropy;
		    //if(index==2)
		      //System.out.println(k+"  "+total_entropy);
		    
		    if(temp < IGT)
		    {
		    	temp = IGT;
		    	index2 = k;
		    }
		  
		  k += 5; /* 5 increment step*/
		}
		
        
        	
		
		return index2;	
	}
	
	public double calculateTempOutlookEntropy(int row,int index)
	{
		int i, neg, pos, index2;
        double numerator, denominator, total, ign, temp;
        
        temp = -65535;
        index2 = -1;
	    for(i=0; i<ROW_ATTRIBUTE; i++)
	    {
	       neg=0;
	       pos=0;
	       
	       if(count_attribute[i][index]>0)
			   pos+=count_attribute[i][index];
		   else if(count_attribute[i][index]<0)
			   neg+=(count_attribute[i][index]*(-1));
	       
	       
	       if(pos == 0 || neg == 0)
		    	  ign = IG;
		    else if (pos == neg)
		    	  ign = IG - 1;
		    	 //ign = IG;
		    else
		    {
		      numerator = pos;
		      denominator = neg;
		      total = numerator + denominator;
		      ign = - ((numerator/total)*(Math.log10(numerator/total)/Math.log10(2.0))+
			    		(denominator/total)*(Math.log10(denominator/total)/Math.log10(2.0)));
		      ign =	IG -ign;
		    }
            if (temp < ign)
            {
               temp = ign;
               index2 = i;
            }
	    	
	    }
		 	   	
		
		return index2;
		
	}
	
	/*Tree data structure*/
	class TreeNode 
	{
		public double entropy;
		public int attribute;
		public TreeNode []children;
		public TreeNode parent;
		public int decision; /* -1 = no, 1= yes, 2= expand*/
		public TreeNode() 
		{
			
		}
		
	}
    
	
	/*Root node creation*/
	TreeNode root = new TreeNode();
	
	
	/*This method recursively expands the tree*/
	public void expandTree(TreeNode node, int index)
	{
	    /*Return if entropy is zero or 1*/	
		
		
			/*expand the node*/
			
			if(index == 0 || index == 1 || index == 2)
		    {
		    	if(index==0)
				  System.out.println("overcast has the following children and their entropy and decision");		    	
		    	if(index==1)
					System.out.println("rainy has the following children and their entropy and decision");		    	
		    	if(index==2)
					System.out.println("sunny has the following children and their entropy and decision");		    	
						
		    	
		    	int i = getBestEntropyForOutlook(index);
				 
				 node.children = new TreeNode[2];
				 node.children[0] = new TreeNode(); /*does not expand further; its a 2 level tree*/
				 node.children[0].parent = node;
				 node.children[0].attribute = i-1;
				 node.children[0].entropy = calculateTempEntropy(index,i-1);
				 node.children[0].decision = findDecision(index,i-1);
				 if(node.children[0].decision >= 0)
				   System.out.println("Temperature <= "+i +" entropy: "+node.children[0].entropy+" decision :"+ "yes");
				 else if(node.children[0].decision < 0)
					 System.out.println("Temperature <= "+i +" entropy: "+node.children[0].entropy+" decision :"+ "no");	 
					 
				 node.children[1] = new TreeNode(); /*does not expand further; its a 2 level*/
				 node.children[1].parent = node;
				 node.children[1].attribute = i;
				 node.children[1].entropy = calculateTempEntropy(index, i);
				 node.children[1].decision = findDecision(index,i);
				 if(node.children[1].decision >= 0)
					   System.out.println("Temperature > "+i +" entropy: "+node.children[1].entropy+" decision :"+ "yes");
				else if(node.children[1].decision < 0)
						 System.out.println("Temperature > "+i +" entropy: "+node.children[1].entropy+" decision :"+ "no");	 
					
				 
				 
			     /*expandTree(node,index);*/ 	
				
				
		    
		    }	
			else if((index+1)%5==0)
			{
				System.out.println("2nd level output");
				node.children = new TreeNode[ROW_ATTRIBUTE];
				System.out.println("The root is the temp <= "+index+1);
				for (int j=0; j< ROW_ATTRIBUTE; j++) 
				{
				
					node.children[j] = new TreeNode(); /*does not expand further; its a 2 level*/
					node.children[j].parent = node;
					node.children[j].attribute = index;
					node.children[j].entropy = calculateTempEntropy(j,index-1);
					node.children[j].decision = findDecision(j,index-1);
					if(j==0)
					{
						if(node.children[j].decision >= 0)
							System.out.println("overcast has the entropy "+node.children[j].entropy+" decision "+ "yes");
						else
							System.out.println("overcast has the entropy "+node.children[j].entropy+" decision "+ "no");
					}
					if(j==1)
					{
						if(node.children[j].decision >= 0)
							System.out.println("rainy has the entropy "+node.children[j].entropy+" decision "+ "yes");
						else
							System.out.println("rainy has the entropy "+node.children[j].entropy+" decision "+ "no");
					}
					if(j==2)
					{
						if(node.children[j].decision >= 0)
							System.out.println("sunny has the entropy "+node.children[j].entropy+" decision "+ "yes");
						else
							System.out.println("sunny has the entropy "+node.children[j].entropy+" decision "+ "no");
					}
					
					
				}
				
				
			}
			else
			{
				
				System.out.println("2nd level output");
				node.children = new TreeNode[ROW_ATTRIBUTE];
				System.out.println("The root is the temp > "+index);
				for (int j=0; j< ROW_ATTRIBUTE; j++) 
				{
				
					node.children[j] = new TreeNode(); /*does not expand further; its a 2 level*/
					node.children[j].parent = node;
					node.children[j].attribute = index;
					node.children[j].entropy = calculateTempEntropy(j,index);
					node.children[j].decision = findDecision(index,j);
					
					if(j==0)
					{
						if(node.children[j].decision >= 0)
							System.out.println("overcast has the entropy "+node.children[j].entropy+" decision "+ "yes");
						else
							System.out.println("overcast has the entropy "+node.children[j].entropy+" decision "+ "no");
					}
					if(j==1)
					{
						if(node.children[j].decision >= 0)
							System.out.println("rainy has the entropy "+node.children[j].entropy+" decision "+ "yes");
						else
							System.out.println("rainy has the entropy "+node.children[j].entropy+" decision "+ "no");
					}
					if(j==2)
					{
						if(node.children[j].decision >= 0)
							System.out.println("sunny has the entropy "+node.children[j].entropy+" decision "+ "yes");
						else
							System.out.println("sunny has the entropy "+node.children[j].entropy+" decision "+ "no");
					}
				
				     	
					
				}
				
                  				
			}
			
			
		
		
		
	}
	
	/*This method calls the expand tree method*/
	public void createDecisionTree(int index)
	{
	    System.out.println("Create root node");
	    if(index == 0 || index == 1 || index == 2)
	    {
	    	
	    	root.children = new TreeNode[ROW_ATTRIBUTE];
	    	System.out.println("The attribute is outlook at the root level");
	    	
	    	for (int j=0; j< ROW_ATTRIBUTE; j++) 
			{
			
				root.children[j] = new TreeNode();
				root.children[j].parent = root;
				root.children[j].attribute = j;
				root.children[j].entropy = calculateOutlookEntropy(j);
				root.children[j].decision = 2;
				
				if(root.children[j].attribute == 0)
				  System.out.println("The root is overcast & entropy: "+root.children[j].entropy+" ");
				else if(root.children[j].attribute == 1)
					  System.out.println("The root is rainy & entropy: "+root.children[j].entropy+" ");
				else if(root.children[j].attribute == 2)
					System.out.println("The root is sunny & entropy: "+root.children[j].entropy+" ");
			    expandTree(root.children[j],j); 	
				
			}
	    	
	    }
	    else 
	    {
	           root.children = new TreeNode[2];
	    	
	       
	    	   System.out.println("The attribute is temp at the root and value " + index);
	    	   
	    	   /*left subtree*/
	    	   root.children[0] = new TreeNode();
	    	   root.children[0].parent = root;
	    	   root.children[0].attribute = index;
	    	   root.children[0].entropy = calculateTotalTempEntropy(index-1);
			   root.children[0].decision = 2;
			   System.out.println("The root is the temp less than eq to "+index
						+" & entropy "+root.children[0].entropy+" ");
			   expandTree(root.children[0],index-1);	   
	    	   /*right subtree*/
	    	   root.children[1] = new TreeNode();
	    	   root.children[1].parent = root;
	    	   root.children[1].attribute = index;
	    	   root.children[1].entropy = calculateTotalTempEntropy(index+1);
			   root.children[1].decision = 2;
			   System.out.println("The root is the temp greater than "+index
						+" & entropy "+root.children[0].entropy+" ");
			   expandTree(root.children[1],index+1);	   
	    	   
	    	   
	       }
	       
	    
	    
	}
		
	
	
	public static void main(String[] args) throws IOException 
	{
		// TODO Auto-generated method stub
		DTree dtree = new DTree();
		dtree.readDataFile();
		int index=dtree.calculateTotalInformationGain();
		dtree.createDecisionTree(index);
		  		  
	}
	
	
		
  }		
		
		
		
	

