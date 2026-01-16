import java.io.*;

public class Search_Alphabet_10809 {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
			String[] word;
			int[] point = new int[26];
			String str ="";
			
			str = br.readLine();
			word = str.split("");
			
			for(int i=0; i<26; i++) {
				point[i] = -1;	
			}
			
			for(int j=0; j< word.length; j++) {
				if(word[j].equals("a")) {
					if(point[0] == -1) {
						point[0] = j; 
					}
				}
				else if(word[j].equals("b")) {
					if(point[1] == -1) {
					   point[1] = j; 
					}
				}
				else if(word[j].equals("c")) {
					if(point[2] == -1) {
					   point[2] = j; 
					}	
				}
			   else if(word[j].equals("d")) {
					if(point[3] == -1) {
					   point[3] = j; 
						}
			   }
			   else if(word[j].equals("e")) {
					if(point[4] == -1) {
					 point[4] = j; 
					    }
			   }
			   else if(word[j].equals("f")) {
					if(point[5] == -1) {
						point[5] = j; 
					} 
			   }
			   else if(word[j].equals("g")) {
					if(point[6] == -1) {
					point[6] = j; 
					}
			   }
			   else if(word[j].equals("h")) {
					if(point[7] == -1) {
					   point[7] = j; 
					} 		
			   }
					else if(word[j].equals("i")) {
						if(point[8] == -1) {
						   point[8] = j; 
						} 		
					}
						else if(word[j].equals("j")) {
							if(point[9] == -1) {
							   point[9] = j; 
							} 		
						}
							else if(word[j].equals("k")) {
								if(point[10] == -1) {
								   point[10] = j; 
								} 	
							}
								else if(word[j].equals("l")) {
									if(point[11] == -1) {
									   point[11] = j; 
									} 	
								}
									else if(word[j].equals("m")) {
										if(point[12] == -1) {
										   point[12] = j; 
										} 	
									}
										else if(word[j].equals("n")) {
											if(point[13] == -1) {
											   point[13] = j; 
											} 	
										}
											else if(word[j].equals("o")) {
												if(point[14] == -1) {
												   point[14] = j; 
												} 	
											}
												else if(word[j].equals("p")) {
													if(point[15] == -1) {
													   point[15] = j; 
													} 	
												}
													else if(word[j].equals("q")) {
														if(point[16] == -1) {
														   point[16] = j; 
														} 	
													}
														else if(word[j].equals("r")) {
															if(point[17] == -1) {
															   point[17] = j; 
															} 	
														}
															else if(word[j].equals("s")) {
																if(point[18] == -1) {
																   point[18] = j; 
																} 	
															}
																else if(word[j].equals("t")) {
																	if(point[19] == -1) {
																	   point[19] = j; 
																	} 	
																}
																	else if(word[j].equals("u")) {
																		if(point[20] == -1) {
																		   point[20] = j; 
																		} 	
																	}
																		else if(word[j].equals("v")) {
																			if(point[21] == -1) {
																			   point[21] = j; 
																			} 	
																		}
																			else if(word[j].equals("w")) {
																				if(point[22] == -1) {
																				   point[22] = j; 
																				} 
																			}
																			else if(word[j].equals("x")) {
																					if(point[23] == -1) {
																						   point[23] = j; 
																						} 	
																			}
																					else if(word[j].equals("y")) {
																						if(point[24] == -1) {
																						   point[24] = j; 
																						} 	
																					}
																						else if(word[j].equals("z")) {
																							if(point[25] == -1) {
																							   point[25] = j; 
																							} 	
																						}
			}
			for(int i=0; i<26; i++) {
				bw.write(Integer.toString(point[i])+" ");
			}
			bw.flush();
	}
}
		