package test;

public class Batch {
	public static void main(String[] args) {
		float matrix[][]={{1,4},{2,5},{5,1},{4,2}};
		float result[]={19,26,19,20};
		float theta[]={2,5};
		float learning_rate = (float) 0.01;
		float loss = (float) 1000.0; 
		for(int i = 0;i<100&&loss>0.0001;++i)
		{
				float error_sum[] = {0,0};
					for(int j = 0;j<4;++j)
						{
							float h = (float) 0.0;
							for(int k=0;k<2;++k)
									{
										h += matrix[j][k]*theta[k];
									}
									error_sum [0]+= (result[j]-h)*matrix[j][0]; //要用到所有的样本进行求和，批梯度下降
									error_sum [1]+= (result[j]-h)*matrix[j][1]; 
									for(int k=0;k<2;++k)
									{
										theta[k] += learning_rate*(error_sum[k]); // 对theta的叠加，修改theta的值 
									}
							}
							System.out.println("*********************************\n");
							System.out.println(theta[0]+","+theta[1]);
							loss = (float) 0.0;
							for(int j = 0;j<4;++j)
							{
									float sum=(float) 0.0;
									for(int k = 0;k<2;++k)
									{
										sum += matrix[j][k]*theta[k];
									}
									loss += (sum-result[j])*(sum-result[j]);
							}
							System.out.println(loss);
				}
		}
	}
