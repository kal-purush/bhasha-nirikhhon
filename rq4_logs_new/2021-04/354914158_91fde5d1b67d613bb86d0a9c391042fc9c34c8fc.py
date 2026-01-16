from config import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import wraps
from pyspark.sql import SQLContext, SparkSession, Window, DataFrame

# 1. Utility Functions

def get_s3_paths_all(meta: dict, title: str):
    path_suffix = tuple([meta[title]['premiere'], meta[title]['tconst'].upper()])
    recsys_date = meta[title]['premiere'][:6] + '01'
    s3_pcas_ = "s3://studiosresearch-projects/recsys_customer_embeddings_pca10pc/date=%s/mkt1/" % (recsys_date)
    s3_covs_ = 's3://research-tmp-test/Portfolio_Optimization_CEM_PS_MKT1_US-%s-%s/' % path_suffix
    s3_hvas_ = 's3://research-tmp-test/CEM_Samples/Customers_Watched_%s-%s/' % path_suffix[::-1]
    
    if dsi_window=='F12M':
#         s3_rois_ = 's3://research-tmp-test/CEM_Samples/revenue_profit_%s-%s/' % path_suffix[::-1]
        s3_rois_ = 's3://research-tmp-test/CEM_Samples/financials/revenue_profit_extended_F12M_%s-%s/' % path_suffix[::-1]
    elif dsi_window=='F3M':
        s3_rois_ = 's3://research-tmp-test/CEM_Samples/revenue_profit_F3M_%s-%s/' % path_suffix[::-1]
    else:
        s3_rois_ = None
    
    return (s3_pcas_, s3_covs_, s3_rois_, s3_hvas_)

def progress(step:int, message:str = "", total:int = 10, title:str = ''):
    if step < 0:
        print('%s: estimating [%s] DSI of title viewing [%s] post premiere' % (title, dsi_window, trt_window))
        print(' -- DR categorical --  :' + ', '.join(DR_covs[0]))
        print(' -- DR continuous  --  :' + ', '.join(DR_covs[1]))
    else:
        print('==='*(step)+'   '*(total-step)+': '+message)

# not doing CEM filtering anymore
# def remove_homogeneous_strata(data_frame, strata_label, HVA_indicator):
#     """
#     Removes strata where high value action is 0 for all customers

#     PARAMETERS: 
#         data_frame - dataframe that has already been labeled
#         strata_label - column name for CEM strata label
#         HVA_indicator - column name for high value action indicator
        
#     RETURN:
#         result_list - CEM stratas that were dropped
#         data_frame_filtered - dataframe where strata with all 0's or 1's for high value action removed
#     """
#     result_list = data_frame\
#         .groupBy([strata_label])\
#         .agg(
#             sum(HVA_indicator).alias('strata_sum'),
#             count(lit(1)).alias('strata_count')
#         )\
#         .where((col('strata_sum') == 0) | (col('strata_sum') == col('strata_count')))\
#         .select(strata_label)\
#         .collect()
    
#     result_list = [result_list[i][0] for i in range(len(result_list))]
#     data_frame_filtered = data_frame.where(~col(strata_label).isin(result_list))
    
#     return result_list, data_frame_filtered

# 2. DataFrame UDF's

# Split and add probability of watching show to dataframe
split1_udf = udf(lambda value: value[0].item(), FloatType())
split2_udf = udf(lambda value: value[1].item(), FloatType())

# 3. DataFrame custom methods

# decorator to attach a function to an attribute
def add_attr(cls):
    def decorator(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            f = func(*args, **kwargs)
            return f

        setattr(cls, func.__name__, _wrapper)
        return func

    return decorator

# custom functions
def custom(self):
    @add_attr(custom)
    def join_rois(df2):
        if dsi_window=='F3M':
            return (
                self
                .join(df2, on=['customer_id'], how='left')
                .withColumn('total_hours', col('total_seconds')/3600.)
                .withColumnRenamed('hours', 'hours_wbr')
                .withColumnRenamed('overall_profit', 'Overall_Profit_%s' % dsi_window)
                .na.fill(
                    {
                        'Outcome_Hours_%s' % dsi_window:0,
                        'Overall_Profit_%s' % dsi_window:0,
                        'total_hours':0.
                    }
                )
            )
        if dsi_window=='F12M':
            return (
                self
                .join(df2, on=['customer_id'], how='left')
                .withColumn('total_hours', col('total_seconds')/3600.)
                .withColumnRenamed('hours', 'hours_wbr')
                .withColumnRenamed('overall_profit', 'Overall_Profit_%s' % dsi_window)
                .withColumnRenamed('retail_profit', 'Retail_Profit_%s' % dsi_window)
                .withColumnRenamed('sub_profit', 'Subs_Profit_%s' % dsi_window)
                .withColumn('Subs_AIVchan_Profit_%s' % dsi_window,
                           coalesce(col('aiv_channels_revenue'), lit(0)) -
                           coalesce(col('aiv_channels_cost'), lit(0)))
                .withColumn('Subs_Prime_Profit_%s' % dsi_window,
                           coalesce(col('prime_revenue'), lit(0)) -
                           coalesce(col('prime_cost'), lit(0)))
                .withColumnRenamed('display_ads_revenue', 'DisplayAds_Revenue_%s' % dsi_window)
                .na.fill(
                    {
                        'Outcome_Hours_%s' % dsi_window:0.,
                        'Overall_Profit_%s' % dsi_window:0.,
                        'Retail_Profit_%s' % dsi_window:0.,
                        'Subs_Profit_%s' % dsi_window:0.,
                        'Subs_AIVchan_Profit_%s' % dsi_window:0.,
                        'Subs_Prime_Profit_%s' % dsi_window:0.,
                        'DisplayAds_Revenue_%s' % dsi_window:0.,
                    }
                )
                
            )
    @add_attr(custom)
    def join_hvas(df2):
        return self.join(df2,  on=['customer_id'], how='left')\
                    .na.fill(
                        {
                            'title_fs':0
                        }
                    )
    
    @add_attr(custom)
    def add_cust_status(T6M_active_thres_secs: int=60):
        return self.withColumn('customer_status', 
                when(
                    ((col('prime_sub_type') == 'NPA') | (col('prime_sub_type') == 'None')) &\
                    ((col('cas_segment') == 'Never Streamer') | (col('cas_segment') == 'None')) &\
                    (col('total_secs_T6M') == 0),\
                        'Non-Prime Never Streamer')\
                .when(
                    ((col('prime_sub_type') == 'NPA') | (col('prime_sub_type') == 'None')) &\
                    (col('cas_segment') != 'Never Streamer') & (col('cas_segment') != 'None') &\
                    (col('total_secs_T6M') == 0),\
                        'Non-Prime Lapsed')\
                .when(
                    ((col('prime_sub_type') == 'NPA') | (col('prime_sub_type') == 'None')) &\
                    (col('total_secs_T6M') >= T6M_active_thres_secs),\
                        'Non-Prime Active')\
                .when(
                    (col('prime_sub_type') != 'NPA') & (col('prime_sub_type') != 'None') &\
                    ((col('cas_segment') == 'Never Streamer') | (col('cas_segment') == 'None')) &\
                    (col('total_secs_T6M') == 0),\
                        'Prime Never Streamer')\
                .when(
                    (col('prime_sub_type') != 'NPA') & (col('prime_sub_type') != 'None') &\
                    (col('cas_segment') != 'Never Streamer') & (col('cas_segment') != 'None') &\
                    (col('total_secs_T6M') == 0),\
                        'Prime Lapsed')\
                .when(
                    (col('prime_sub_type') != 'NPA') & (col('prime_sub_type') != 'None') &\
                    (col('total_secs_T6M') >= T6M_active_thres_secs),\
                        'Prime Active')\
                .otherwise('Not Categorized'))
    
    @add_attr(custom)
    def add_hva_binary():
        return self\
            .withColumn('Cops_Crime_Capers', when(col('segment_genre_id') == '9900', 1).otherwise(0))\
            .withColumn('World_Creation_Action', when(col('segment_genre_id') == '9901', 1).otherwise(0))\
            .withColumn('Reality_Game', when(col('segment_genre_id') == '9902', 1).otherwise(0))\
            .withColumn('Lit_Based_Soaps', when(col('segment_genre_id') == '9903', 1).otherwise(0))\
            .withColumn('Big_City_Rel_Drama_Comedy', when(col('segment_genre_id') == '9904', 1).otherwise(0))\
            .withColumn('Family_Anim_Adventure', when(col('segment_genre_id') == '9905', 1).otherwise(0))\
            .withColumn('Preschool_Anim', when(col('segment_genre_id') == '9906', 1).otherwise(0))\
            .withColumn('Mystery_Drama', when(col('segment_genre_id') == '9907', 1).otherwise(0))\
            .withColumn('Supnatual_Hor_Myst', when(col('segment_genre_id') == '9908', 1).otherwise(0))\
            .withColumn('Age_18_24', when(col('Max_prob_age') == 'prob_age1', 1).otherwise(0))\
            .withColumn('Age_25_34', when(col('Max_prob_age') == 'prob_age2', 1).otherwise(0))\
            .withColumn('Age_35_44', when(col('Max_prob_age') == 'prob_age3', 1).otherwise(0))\
            .withColumn('Age_45_54', when(col('Max_prob_age') == 'prob_age4', 1).otherwise(0))\
            .withColumn('Age_55_64', when(col('Max_prob_age') == 'prob_age5', 1).otherwise(0))\
            .withColumn('Age_65+', when(col('Max_prob_age') == 'prob_age6', 1).otherwise(0))\
            .withColumn('CREAM_0_Active', when(
                (col('CREAM_Hours') == 0) &\
                ((col('customer_status') != 'Non-Prime Never Streamer') &\
                 (col('customer_status') != 'Non-Prime Lapsed') &\
                 (col('customer_status') != 'Prime Lapsed') &\
                 (col('customer_status') != 'Prime Never Streamer')), 1).otherwise(0))\
            .withColumn('CREAM_1_to_4', when(
                (col('CREAM_Hours') >= 1) & (col('CREAM_Hours') <= 4), 1).otherwise(0))\
            .withColumn('Prime_NSFS_total', when(
                (col('customer_status') == 'Prime Never Streamer') & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | (col('treated') == 0)), 1).otherwise(0))\
            .withColumn('Prime_CREAM1_NS_Lapsed', when(
                ((col('customer_status') == 'Prime Never Streamer') | (col('customer_status') == 'Prime Lapsed')) & \
                (((col('title_fs') != 1) & (col('treated') == 1)) | (col('treated') == 0)), 1).otherwise(0))\
            .withColumn('Prime_NSFS_incre', when(
                (col('customer_status') == 'Prime Never Streamer') & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | ((col('treated') == 0) & (col('Outcome_Hours_F1M') > 0))), 1
                ).otherwise(0))\
            .withColumn('Prime_NSFS_non_incre', when(
                (col('customer_status') == 'Prime Never Streamer') & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | ((col('treated') == 0) & (col('Outcome_Hours_F1M') == 0))), 1
                ).otherwise(0))\
            .withColumn('Prime_Lapse_RE_total', when(
                (col('customer_status') == 'Prime Lapsed') & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | (col('treated') == 0)), 1).otherwise(0))\
            .withColumn('Prime_Lapse_RE_incre', when(
                (col('customer_status') == 'Prime Lapsed') & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | ((col('treated') == 0) & (col('Outcome_Hours_F1M') > 0))), 1
                ).otherwise(0))\
            .withColumn('Prime_Lapse_RE_non_incre', when(
                (col('customer_status') == 'Prime Lapsed') & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | ((col('treated') == 0) & (col('Outcome_Hours_F1M') == 0))), 1
                ).otherwise(0))\
            .withColumn('NonPrime_TPS_total', when(
                ((col('customer_status') == 'Non-Prime Never Streamer') | (col('customer_status') == 'Non-Prime Lapsed')) & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | (col('treated') == 0)), 1).otherwise(0))\
            .withColumn('NonPrime_CREAM1_NS_Lapsed', when(
                ((col('customer_status') == 'Non-Prime Never Streamer') | (col('customer_status') == 'Non-Prime Lapsed')) & \
                (((col('title_fs') != 1) & (col('treated') == 1)) | (col('treated') == 0)), 1).otherwise(0))\
            .withColumn('NonPrime_TPS_incre', when(
                ((col('customer_status') == 'Non-Prime Never Streamer') | (col('customer_status') == 'Non-Prime Lapsed')) & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | ((col('treated') == 0) & (col('Outcome_Hours_F1M') > 0))), 1
                ).otherwise(0))\
            .withColumn('NonPrime_TPS_non_incre', when(
                ((col('customer_status') == 'Non-Prime Never Streamer') | (col('customer_status') == 'Non-Prime Lapsed')) & \
                (((col('title_fs') == 1) & (col('treated') == 1)) | ((col('treated') == 0) & (col('Outcome_Hours_F1M') == 0))), 1
                ).otherwise(0))
    
    @add_attr(custom)
    def quantile_bucketize(col_list: list, num_buckets : int = 3):
        
        from pyspark.ml.feature import QuantileDiscretizer
        df = self
        
        for c in col_list:
            if c in df.schema.names:
                non_zero_values = df.select(c).where(col(c)!=0)
                bucketizer = QuantileDiscretizer(
                    numBuckets = num_buckets,
                    inputCol = c,
                    outputCol = c+"_bucket"
                    ).fit(non_zero_values).setHandleInvalid('keep')
                df = bucketizer.transform(df).drop(c)
        return df
    
    @add_attr(custom)
    def drop_const_cols(cat_cols: list, ctn_cols: list, thres: float = 1e-2):
        
        from pyspark.sql.functions import stddev, countDistinct
        
        cat_cols = [col for col in cat_cols if col in self.schema.names]
        ctn_cols = [col for col in ctn_cols if col in self.schema.names]
        drop_cols = []
        
        if not cat_cols and not ctn_cols:
            return drop_cols, self
        
        # constant categorical when distinct value = 1
        if cat_cols:
            constant_categorical_cols = self.agg(*(countDistinct(c).alias(c) for c in self.select(cat_cols).columns)).collect()[0]
            drop_cols += [c for c in constant_categorical_cols.asDict() if constant_categorical_cols[c]==1]
        
        # constant continuous when stddev <= thres (default 1e-2)
        if ctn_cols:
            constant_continuous_cols = self.agg(*(stddev(c).alias(c) for c in self.select(ctn_cols).columns)).collect()[0]
            drop_cols += [c for c in constant_continuous_cols.asDict() if constant_continuous_cols[c] <= 1e-2]
        
        return drop_cols, self.drop(*drop_cols)
    
    @add_attr(custom)
    def rename_pcas(pca_cols: list):
        df = self
        for c in pca_cols:
            if c in df.schema.names:
                df = df.withColumnRenamed(c, c.replace('[','').replace(']',''))
        return df
    
    @add_attr(custom)
    def multi_category_to_string(col_list: list):
        df = self
        for c in col_list:
            if c in self.schema.names:
                df = df.withColumn(c, col(c).cast('string'))
        return df
    
    @add_attr(custom)
    def cols_calibrate(cols_dict: dict, to_print: bool = True):
        cols_dcit_copy = cols_dict
        include_cols = set()
        for k, col_list in cols_dict.items():
            if not k.startswith('drop'):
                cols_in_df = [col for col in col_list if col in self.schema.names]
                cols_dcit_copy[k] = cols_in_df
                include_cols = include_cols.union(set(cols_in_df))
        exclude_cols = list(set(self.schema.names) - include_cols)
        cols_dcit_copy['dropped_no_use'] = exclude_cols

        return cols_dcit_copy, self.drop(*exclude_cols)
    
    @add_attr(custom)
    def fill_with_mean_inclusive(include: set = set()): 
        inclusive_means = self.agg(*(avg(c).alias(c) for c in self.columns if c in include))
        return self.na.fill(inclusive_means.first().asDict())

    return custom

## attach custom methods to DataFrame
DataFrame.custom = property(custom)

# 4. Estimator Functions

## Propensity model
def add_propensity(df: DataFrame, lhs: str, rhs: list, ps_col: str, regParam: float = 1e-2):
    
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import RFormula
    
    PS_formula = RFormula(
        formula = '%s ~ %s' % (lhs, ' + '.join(rhs)),
        featuresCol="features",
        labelCol="label"
    )
    
    PS_df = PS_formula\
        .fit(df)\
        .transform(df.select(['customer_id', lhs] + rhs))\
        .select('customer_id', 'features', 'label')
    
    lr = LogisticRegression(
        featuresCol='features',
        labelCol = 'label',
        tol=1e-4, regParam=regParam, elasticNetParam=0.3
    )
    
    preds = lr\
                .fit(PS_df).transform(PS_df)\
                .select(['customer_id', 'probability', 'label'])\
                .withColumn(ps_col, split2_udf('probability'))\
                .drop('probability')
    mean_PS = preds.rollup('label').mean(ps_col).alias('mean_ps').collect()
    mean_PS = [x.asDict() for x in mean_PS]
    
    mean_trt = preds.rollup('label').count().collect()
    mean_trt = [x.asDict() for x in mean_trt]
    
    df = df.join(preds.drop('label'), on=['customer_id'], how='inner')
    
    return df, mean_PS, mean_trt

## DSI Regression model: Y ~ trt + PS + covs
def dsi_regression(df: DataFrame, dsi: str, trt: str, ps: str, cov_list: list, regParam: float = 1e-2):
    
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import RFormula
    
    if ps:
        rhs_ls = [trt, ps] + cov_list
    else:
        rhs_ls = [trt] + cov_list
    
    dsi_formula = RFormula(
        formula = '%s ~ %s' % (dsi, ' + '.join(rhs_ls)),
        featuresCol="features",
        labelCol="label"
    )
    
    dsi_df = dsi_formula\
        .fit(df)\
        .transform(df.select(['customer_id', dsi] + rhs_ls))\
        .select('customer_id', 'features', 'label')
    
    df_stats = df.filter(col(trt) > 0).select(
        mean(col('Treated_F1M')).alias('mean_dosage'),
        count(lit(1)).alias('total_treated')
    ).collect()[0].asDict()
    
    lr = LinearRegression(
        featuresCol='features',
        labelCol = 'label',
        tol=1e-4, regParam=regParam, elasticNetParam=0.5
    )
    lrm = lr.fit(dsi_df)
    
    return lrm.coefficients, df_stats

## Doubly robust Regression model: Y ~ trt + iptw + covs
def dr_regression(df: DataFrame, dsi: str, trt: str, ps: str, cov_list: list, 
                  regParam: float = 1e-2, max_iptw: float = 1e-4):
    
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import RFormula
    
    if not dsi or not trt or not ps:
        return
#     if df.count() < 1000:
#         return
    
    _, df = df.custom.drop_const_cols(cov_list[0], cov_list[1])
    
    if cov_list:
        flat_cov_list = [x for sublist in cov_list for x in sublist]
        flat_cov_list = [c for c in flat_cov_list if c in df.schema.names]
    
    df = (
        df
        .withColumn('iptw', 1./((col(trt)*col(ps)+(1-col(trt))*(1-col(ps)))+1e-4))
        .withColumn('ipt0', 1./(1-col(ps)+1e-4))
        .cache()
    )
    rhs_ls = [trt, 'iptw'] + flat_cov_list
    
    transformer = RFormula(
        formula='%s ~ %s' % (dsi, ' + '.join(rhs_ls)),
        featuresCol="features",
        labelCol="label"
    ).fit(df)
    
    dsi_df = (
        transformer
        .transform(df.select(['customer_id', dsi] + rhs_ls))\
        .select('customer_id', 'features', 'label')
    )
    
    lr = LinearRegression(
        featuresCol='features',
        labelCol = 'label',
        tol=1e-4, regParam=regParam, elasticNetParam=0.5
    )
    lrm = lr.fit(dsi_df)
    
    dsi_1 = lrm.transform(
        transformer
        .transform(
            df
            .filter(col(trt) > 0.)
            .select(['customer_id', dsi] + rhs_ls)
        ).select('customer_id', 'features', 'label')
    ).select('customer_id', col('prediction').alias('dsi_1'))
    
    dsi_0 = lrm.transform(
        transformer
        .transform(
            df
            .filter(col(trt) > 0.)
            .withColumn(trt, 1.-col(trt))
            .drop('iptw').withColumnRenamed('ipt0', 'iptw')
            .select(['customer_id', dsi] + rhs_ls)
        ).select('customer_id', 'features', 'label')
    ).select('customer_id', col('prediction').alias('dsi_0'))
    
    estimates = (
        dsi_1
        .join(dsi_0, on=['customer_id'], how='inner')
        .withColumn('effect', col('dsi_1') - col('dsi_0'))
        .agg(
            mean(col('effect')).alias('att_mean'),
            expr('percentile(effect, array(0.5))')[0].alias('att_median'),
            stddev(col('effect')).alias('att_std'),
            count(col('customer_id')).alias('total_trt')
        ).collect()
    )
    
    return estimates

def hva_estimate(df, dsi_list, hva):
    df_hva = df.filter(col(hva) > 0).cache()
    result = {}
    if df_hva.count() < 1000:
        return hva, result
    for dsi in dsi_list:
        est = dr_regression(df_hva, dsi, 'treated', 'ps_treated', DR_covs)
        result[dsi] = est[0].asDict()
    df_hva = df_hva.unpersist()
    return hva, result