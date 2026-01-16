
package  com.zn.zxw.intelligencize.model;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
  	/**
	*��ע:�������Ѽ�¼��
	*/
    public class AccumulativeIntegraListInfo implements Serializable
    {
        public AccumulativeIntegraListInfo()
        { }
		
		
		/**
		*���
		*/
		private String   _id;//���
		/**
		*�߼����
		*/
		private String   _accumulativeIntegraListId;//�߼����
		/**
		*��������
		*/
		private int   _cousumeQuantity;//��������
		/**
		*����ʱ��
		*/
		private String   _date;//����ʱ��
		/**
		*�Ƿ�֧��(1��true��0��false)
		*/
		private String   _expenditure;//�Ƿ�֧��(1��true��0��false)
		/**
		*���Ѷ����
		*/
		private String   _targetTable;//���Ѷ����
		/**
		*����Ŀ��
		*/
		private String   _consumeTarget;//����Ŀ��
		/**
		*Ԥ���ֶ�
		*/
		private String   _field;//Ԥ���ֶ�
		private  CommodityInformationInfo  _commodityInformationInfo;
		
		//list FK
		
		
		
		
		/**
		*���
		*/
		public String getId()
		{
		
			 return _id; 
		}
		/**
		*���
		*/
		public void  setId (String  Id )
		{
			_id = Id ;
		}
		
		
		/**
		*�߼����
		*/
		public String getAccumulativeIntegraListId()
		{
		
			 return _accumulativeIntegraListId; 
		}
		/**
		*�߼����
		*/
		public void  setAccumulativeIntegraListId (String  AccumulativeIntegraListId )
		{
			_accumulativeIntegraListId = AccumulativeIntegraListId ;
		}
		
		
		/**
		*��������
		*/
		public int getCousumeQuantity()
		{
		
			 return _cousumeQuantity; 
		}
		/**
		*��������
		*/
		public void  setCousumeQuantity (int  CousumeQuantity )
		{
			_cousumeQuantity = CousumeQuantity ;
		}
		
		
		/**
		*����ʱ��
		*/
		public String getDate()
		{
		
			 return _date; 
		}
		/**
		*����ʱ��
		*/
		public void  setDate (String  Date )
		{
			_date = Date ;
		}
		
		
		/**
		*�Ƿ�֧��(1��true��0��false)
		*/
		public String getExpenditure()
		{
		
			 return _expenditure; 
		}
		/**
		*�Ƿ�֧��(1��true��0��false)
		*/
		public void  setExpenditure (String  Expenditure )
		{
			_expenditure = Expenditure ;
		}
		
		
		/**
		*���Ѷ����
		*/
		public String getTargetTable()
		{
		
			 return _targetTable; 
		}
		/**
		*���Ѷ����
		*/
		public void  setTargetTable (String  TargetTable )
		{
			_targetTable = TargetTable ;
		}
		
		
		/**
		*����Ŀ��
		*/
		public String getConsumeTarget()
		{
		
			 return _consumeTarget; 
		}
		/**
		*����Ŀ��
		*/
		public void  setConsumeTarget (String  ConsumeTarget )
		{
			_consumeTarget = ConsumeTarget ;
		}
		
		
		/**
		*Ԥ���ֶ�
		*/
		public String getField()
		{
		
			 return _field; 
		}
		/**
		*Ԥ���ֶ�
		*/
		public void  setField (String  Field )
		{
			_field = Field ;
		}
		
	
		
		
		
		public CommodityInformationInfo  getCommodityInformationInfo()
		{
		return _commodityInformationInfo;
		}
		
		public void  setCommodityInformationInfo(CommodityInformationInfo  CommodityInformationInfo)
		{
		 _commodityInformationInfo =CommodityInformationInfo;
		
		}
		
		
				//list FK
  }
 
	
		


