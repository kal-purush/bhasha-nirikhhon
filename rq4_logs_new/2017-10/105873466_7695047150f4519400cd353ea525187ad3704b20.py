# This module is not included into the sample project
from sklearn.base import TransformerMixin, BaseEstimator
from pipelines import *
import numpy as np
import pandas as pd

from sklearn.pipeline import make_pipeline

from sklearn.model_selection import train_test_split


#
# Definition of original dataset fields
#
CATEGORICAL_ATTRS = {
    'MSSubClass': ['20', '30', '40', '45', '50', '60', '70', '75', '80', '85', '90', '120', '150', '160', '180', '190'],
    # Identifies the type of dwelling involved in the sale.
    'MSZoning': ['A', 'C', 'FV', 'I', 'RH', 'RL', 'RP', 'RM'],
    # Identifies the general zoning classification of the sale.
    'Street': ['Grvl', 'Pave'],  # Type of road access to property
    'Alley': ['Grvl', 'Pave', 'NA'],  # Type of alley access to property
    'LotShape': ['Reg', 'IR1', 'IR2', 'IR3'],  # General shape of property
    'LandContour': ['Lvl', 'Bnk', 'HLS', 'Low'],  # Flatness of the property
    'Utilities': ['AllPub', 'NoSewr', 'NoSeWa', 'ELO'],  # Type of utilities available
    'LotConfig': ['Inside', 'Corner', 'CulDSac', 'FR2', 'FR3'],  # Lot configuration
    'LandSlope': ['Gtl', 'Mod', 'Sev'],  # Slope of property
    'Neighborhood': ['Blmngtn', 'Blueste', 'BrDale', 'BrkSide', 'ClearCr', 'CollgCr', 'Crawfor', 'Edwards', 'Gilbert',
                     'IDOTRR', 'MeadowV', 'Mitchel', 'Names', 'NoRidge', 'NPkVill', 'NridgHt', 'NWAmes', 'OldTown',
                     'SWISU', 'Sawyer', 'SawyerW', 'Somerst', 'StoneBr', 'Timber', 'Veenker'],
    # Physical locations within Ames city limits
    'Condition1': ['Artery', 'Feedr', 'Norm', 'RRNn', 'RRAn', 'PosN', 'PosA', 'RRNe', 'RRAe'],
    # Proximity to various conditions
    'Condition2': ['Artery', 'Feedr', 'Norm', 'RRNn', 'RRAn', 'PosN', 'PosA', 'RRNe', 'RRAe'],
    # Proximity to various conditions (if more than one is present)
    'BldgType': ['1Fam', '2FmCon', 'Duplx', 'TwnhsE', 'TwnhsI'],  # Type of dwelling
    'HouseStyle': ['1Story', '1.5Fin', '1.5Unf', '2Story', '2.5Fin', '2.5Unf', 'SFoyer', 'SLvl'],  # Style of dwelling
    'OverallQual': ['10', '9', '8', '7', '6', '5', '4', '3', '2', '1'],
    # Rates the overall material and finish of the house
    'OverallCond': ['10', '9', '8', '7', '6', '5', '4', '3', '2', '1'],  # Rates the overall condition of the house
    'RoofStyle': ['Flat', 'Gable', 'Gambrel', 'Hip', 'Mansard', 'Shed'],  # Type of roof
    'RoofMatl': ['ClyTile', 'CompShg', 'Membran', 'Metal', 'Roll', 'Tar&Grv', 'WdShake', 'WdShngl'],  # Roof material
    'Exterior1st': ['AsbShng', 'AsphShn', 'BrkComm', 'BrkFace', 'CBlock', 'CemntBd', 'HdBoard', 'ImStucc', 'MetalSd',
                    'Other', 'Plywood', 'PreCast', 'Stone', 'Stucco', 'VinylSd', 'Wd', 'WdShing'],
    # Exterior covering on house
    'Exterior2nd': ['AsbShng', 'AsphShn', 'BrkComm', 'BrkFace', 'CBlock', 'CemntBd', 'HdBoard', 'ImStucc', 'MetalSd',
                    'Other', 'Plywood', 'PreCast', 'Stone', 'Stucco', 'VinylSd', 'Wd', 'WdShing'],
    # Exterior covering on house (if more than one material)
    'MasVnrType': ['BrkCmn', 'BrkFace', 'CBlock', 'None', 'Stone'],  # Masonry veneer type
    'ExterQual': ['Ex', 'Gd', 'TA', 'Fa', 'Po'],  # Evaluates the quality of the material on the exterior
    'ExterCond': ['Ex', 'Gd', 'TA', 'Fa', 'Po'],  # Evaluates the present condition of the material on the exterior
    'Foundation': ['BrkTil', 'CBlock', 'PConc', 'Slab', 'Stone', 'Wood'],  # Type of foundation
    'BsmtQual': ['Ex', 'Gd', 'TA', 'Fa', 'Po', 'NA'],  # Evaluates the height of the basement
    'BsmtCond': ['Ex', 'Gd', 'TA', 'Fa', 'Po', 'NA'],  # Evaluates the general condition of the basement
    'BsmtExposure': ['Gd', 'Av', 'Mn', 'No', 'NA'],  # Refers to walkout or garden level walls
    'BsmtFinType1': ['GLQ', 'ALQ', 'BLQ', 'Rec', 'LwQ', 'Unf', 'NA'],  # Rating of basement finished area
    'BsmtFinType2': ['GLQ', 'ALQ', 'BLQ', 'Rec', 'LwQ', 'Unf', 'NA'],
    # Rating of basement finished area (if multiple types)
    'Heating': ['Floor', 'GasA', 'GasW', 'Grav', 'OthW', 'Wall'],  # Type of heating
    'HeatingQC': ['Ex', 'Gd', 'TA', 'Fa', 'Po'],  # Heating quality and condition
    'CentralAir': ['N', 'Y'],  # Central air conditioning
    'Electrical': ['SBrkr', 'FuseA', 'FuseF', 'FuseP', 'Mix'],  # Electrical system
    'KitchenQual': ['Ex', 'Gd', 'TA', 'Fa', 'Po'],  # Kitchen quality
    'Functional': ['Typ', 'Min1', 'Min2', 'Mod', 'Maj1', 'Maj2', 'Sev', 'Sal'],
    # Home functionality (Assume typical unless deductions are warranted)
    'FireplaceQu': ['Ex', 'Gd', 'TA', 'Fa', 'Po', 'NA'],  # Fireplace quality
    'GarageType': ['2Types', 'Attchd', 'Basment', 'BuiltIn', 'CarPort', 'Detchd', 'NA'],  # Garage location
    'GarageFinish': ['Fin', 'RFn', 'Unf', 'NA'],  # Interior finish of the garage
    'GarageQual': ['Ex', 'Gd', 'TA', 'Fa', 'Po', 'NA'],  # Garage quality
    'GarageCond': ['Ex', 'Gd', 'TA', 'Fa', 'Po', 'NA'],  # Garage condition
    'PavedDrive': ['Y', 'P', 'N'],  # Paved driveway
    'PoolQC': ['Ex', 'Gd', 'TA', 'Fa', 'NA'],  # Pool quality
    'Fence': ['GdPrv', 'MnPrv', 'GdWo', 'MnWw', 'NA'],  # Fence quality
    'MiscFeature': ['Elev', 'Gar2', 'Othr', 'Shed', 'TenC', 'NA'],
    # Miscellaneous feature not covered in other categories
    'SaleType': ['WD', 'CWD', 'VWD', 'New', 'COD', 'Con', 'ConLw', 'ConLI', 'ConLD', 'Oth'],  # Type of sale
    'SaleCondition': ['Normal', 'Abnorml', 'AdjLand', 'Alloca', 'Family', 'Partial'],  # Condition of sale
}

NUMERICAL_ATTRS = [
    'LotFrontage',  # Linear feet of street connected to property
    'LotArea',  # Lot size in square feet
    'YearBuilt',  # Original construction date
    'YearRemodAdd',  # Remodel date (same as construction date if no remodeling or additions)
    'MasVnrArea',  # Masonry veneer area in square feet
    'BsmtFinSF1',  # Type 1 finished square feet
    'BsmtFinSF2',  # Type 2 finished square feet
    'BsmtUnfSF',  # Unfinished square feet of basement area
    'TotalBsmtSF',  # Total square feet of basement area
    '1stFlrSF',  # First Floor square feet
    '2ndFlrSF',  # Second floor square feet
    'LowQualFinSF',  # Low quality finished square feet (all floors)
    'GrLivArea',  # Above grade (ground) living area square feet
    'BsmtFullBath',  # Basement full bathrooms
    'BsmtHalfBath',  # Basement half bathrooms
    'FullBath',  # Full bathrooms above grade
    'HalfBath',  # Half baths above grade
    'BedroomAbvGr',  # Bedrooms above grade (does NOT include basement bedrooms)
    'KitchenAbvGr',  # Kitchens above grade
    'TotRmsAbvGrd',  # Total rooms above grade (does not include bathrooms)
    'Fireplaces',  # Number of fireplaces
    'GarageYrBlt',  # Year garage was built
    'GarageCars',  # Size of garage in car capacity
    'GarageArea',  # Size of garage in square feet
    'WoodDeckSF',  # Wood deck area in square feet
    'OpenPorchSF',  # Open porch area in square feet
    'EnclosedPorch',  # Enclosed porch area in square feet
    '3SsnPorch',  # Three season porch area in square feet
    'ScreenPorch',  # Screen porch area in square feet
    'PoolArea',  # Pool area in square feet
    'MiscVal',  # $Value of miscellaneous feature
    'MoSold',  # Month Sold (MM)
    'YrSold',  # Year Sold (YYYY)

]


class DataFrameCategoricalAverage(BaseEstimator, TransformerMixin):
    def __init__(self, attribute_names):
        self.attribute_names = attribute_names

        self.avg_dict = {}

        if not self.attribute_names:
            raise ValueError("Empty 'attribute_names' for DataFrameSelector")

    def fit(self, X, y=None):
        assert isinstance(X, pd.DataFrame)
        assert y is not None
        assert isinstance(y, pd.Series)

        for col_name in self.attribute_names:
            data_values = X[col_name].fillna('!NA!')
            assert not np.any(pd.isnull(data_values.values)), f"Categorical values of '{col_name}' has NaN"

            # self.avg_dict[col_name] = pd.Series(y.values/X['GrLivArea'].values, index=data_values.values).groupby(level=0).agg(np.mean)
            self.avg_dict[col_name] = pd.Series(y.values, index=data_values.values).groupby(level=0).agg(np.mean)
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)
        df = X.copy()

        for col_name in self.attribute_names:
            try:
                ser = df[col_name].fillna('!NA!')
                avg_series = self.avg_dict[col_name]

                for k, v in avg_series.items():
                    # ser[ser == k] = (v-avg_series.min()) / (avg_series.max() - avg_series.min())
                    ser[ser == k] = v / avg_series.max()
                    # ser[ser == k] = v

                ser.fillna(avg_series.mean(), inplace=True)
                try:
                    # Some categorical values are not filled or NaN
                    df[col_name] = ser.astype(np.float)
                except ValueError:
                    # Fill by average
                    ser = pd.to_numeric(ser, errors='coerce').fillna(avg_series.mean())
                    df[col_name] = ser.astype(np.float)

            except Exception as exc:
                raise Exception(f"Error in processing: {col_name} data. Message: {exc}")
        return df


class DataFrameFeaturesProcessing(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)

        df = X.copy()

        df['!Street'] = df['Street'].map({'Pave': 1.0, 'Grvl': 0.5, float('nan'): 0.0})
        df['!Alley'] = df['Alley'].map({'Pave': 1.0, 'Grvl': 0.5, float('nan'): 0.0})
        df['!LotShape'] = df['LotShape'].map({'Reg': 1.0, 'IR1': 0.75, 'IR2': 0.5, 'IR3': 0.25})
        df['!LandContour'] = df['LandContour'].map({'Lvl': 0.7, 'Bnk': 0.5, 'HLS': 1.0, 'Low': 0.6, float('nan'): 0.0})
        df['!LotConfig'] = df['LotConfig'].map(
            {'CulDSac': 0.1, 'FR3': 0.9, 'FR2': 0.7, 'Corner': 0.6, 'Inside': 0.5, float('nan'): 0.5})

        # TOO many NaN values
        del df['LotFrontage']

        del df['Utilities']
        # LotConfig - categorical?
        df['!LandSlope'] = df['LandSlope'].map({'Gtl': 1.0, 'Mod': 0.5, 'Sev': 0.25, float('nan'): 0.0})

        # Neigbourhood - skipped categorical/avg processing
        # Condition1 - skipped categorical/avg processing
        del df['Condition2']
        # BldgType -  skipped categorical/avg processing
        # HouseStyle - skipped categorical/avg processing
        # OverallQual - used AS IS
        del df['OverallCond']  # Bad clustering
        # YearBuilt - as is
        # YearRemod - as is
        del df['RoofStyle']  # Bad clustering
        del df['RoofMatl']  # Bad clustering
        # Exterior1st - skipped categorical/avg processing
        # Exterior2nd - skipped categorical/avg processing
        # !! Then process  (Exterior1st + Exterior2nd) / 2
        df['!MasVnrType'] = df['MasVnrType'].map(
            {'Stone': 1.0, 'BrkFace': 0.75, 'BrkCmn': 0.5, 'None': 0.0, float('nan'): 0.0})

        QUALITY_MAP = {'Ex': 1.0, 'Gd': 0.75, 'TA': 0.5, 'Fa': 0.25, 'Po': 0.1, 'NA': 0.0, float('nan'): 0.0}
        df['!ExterQual'] = df['ExterQual'].map(QUALITY_MAP)
        df['!ExterCond'] = df['ExterCond'].map(QUALITY_MAP)
        df['!BsmtQual'] = df['BsmtQual'].map(QUALITY_MAP)
        df['!BsmtCond'] = df['BsmtCond'].map(QUALITY_MAP)
        df['!BsmtExposure'] = df['BsmtExposure'].map({'Gd': 1.0, 'Av': 0.75, 'Mn': 0.5, 'No': 0.25, float('nan'): 0.0})

        del df['Heating']  # Bad clustering
        df['!HeatingQC'] = df['HeatingQC'].map(QUALITY_MAP)
        df['!CentralAir'] = df['CentralAir'].map({'Y': 1, 'N': 0, float('nan'): 1})

        # Standard Circuit Breakers & Romex = GOOD (1), other options are BAD (0)
        df['!Electrical'] = df['Electrical'].map(
            {'SBrkr': 1, float('nan'): 0, 'FuseA': 0, 'FuseF': 0, 'FuseP': 0, 'Mix': 0})
        df['!KitchenQual'] = df['KitchenQual'].map(QUALITY_MAP)

        del df['Functional']  # Bad clustering
        df['!FireplaceQu'] = df['FireplaceQu'].map(QUALITY_MAP)
        df['!GarageQual'] = df['GarageQual'].map(QUALITY_MAP)
        df['!GarageCond'] = df['GarageCond'].map(QUALITY_MAP)

        df['!PavedDrive'] = df['PavedDrive'].map({'Y': 0.75, 'P': 0.5, 'N': 0.25, float('nan'): 0.25})

        # PoolArea - no data
        del df['PoolQC']
        del df['PoolArea']

        # No data
        del df['MiscFeature']
        del df['MiscVal']

        return df


class DataFrameExtraFeatures(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)
        df = X.copy()

        #
        # Advanced features engineering
        #

        # Calculate house age from 'YearBuilt' and 'YrSold' features
        df['!HouseAge'] = df['YrSold'] - df['YearBuilt']

        #
        # Integrate multiple features in one
        #
        df['!LandQuality'] = (df['!Street'] +
                              df['!Alley'] +
                              df['!LotShape'] +
                              df['!LandContour'] +
                              df['!LotConfig'] +
                              df['!LandSlope'])
        del df['!Street']
        del df['!Alley']
        del df['!LotShape']
        del df['!LandContour']
        del df['!LotConfig']
        del df['!LandSlope']

        #
        # Calculate MasVnrArea quality feature
        #
        # df['!MasVnrAreaQ'] = df['!MasVnrType'] * df['MasVnrArea']
        # del df['MasVnrArea']
        del df['!MasVnrType']

        #
        # Calculate house exterion integrated feature
        #
        df['!ExretiorScore'] = (df['Exterior1st'] + df['Exterior2nd']) / 2 * (df['!ExterQual'] + df['!ExterCond'])
        # del df['Exterior1st']
        # del df['Exterior2nd']
        del df['!ExterQual']
        del df['!ExterCond']

        return df

#
# Numeric features pipeline definition
#
numeric_pipeline = make_pipeline(
    # Select numeric features from the dataset
    DataFrameSelector(NUMERICAL_ATTRS),

    # Impute missing values (replace NaN by mean())
    DataFrameFunctionTransformer(func=np.mean, impute=True),

    # Transform features values
    DataFrameFunctionTransformer(func=np.log1p),

    # Perform extra sanity check for missing data
    DataFrameBadValuesCheck(),
)

#
# Categorical features pipeline definition
#
categorical_pipeline = make_pipeline(
    # Convert categorical string values to numbers + apply one-hot-encoding
    DataFrameConvertCategorical(CATEGORICAL_ATTRS, out_dataframe=True),

    # Perform extra sanity check for missing data
    DataFrameBadValuesCheck(),
)

#
# Custom features selector
#
custom_features_pipeline = make_pipeline(
    # Select all features with names started with '!' (conventionally these are custom features)
    DataFrameSelector("^!.*"),

    # Perform extra sanity check for missing data
    DataFrameBadValuesCheck(),
)

data_processing_pipeline = DataFrameFeatureUnion([numeric_pipeline, categorical_pipeline, custom_features_pipeline])


def data_process_and_clean_basic(dataset):
    """
    Apply data cleaning and processing pipeline to the dataset
    :param dataset:
    :return:
    """
    return data_processing_pipeline.fit_transform(dataset)


def data_process_and_clean_advanced_features(dataset, target_y):
    """
    Apply data cleaning and processing pipeline to the dataset
    :param dataset:
    :return:
    """
    CATEGORICAL_TO_AVG = ['Neighborhood', 'MSZoning', 'Condition1', 'BldgType', 'HouseStyle',
                          'Exterior1st', 'Exterior2nd', 'Foundation', 'GarageType', 'BsmtFinType1', 'BsmtFinType2',
                          'GarageFinish', 'Fence', 'SaleType', 'SaleCondition'
                          ]

    features_pipeline = make_pipeline(
        # Delete inconsistent and noisy features
        DataFrameFeaturesProcessing(),

        # Replace selected categorcial features by mean SalePrice
        DataFrameCategoricalAverage(CATEGORICAL_TO_AVG),

        # Add new features
        DataFrameExtraFeatures(),

        # Perform general feature cleaning and processing
        data_processing_pipeline
    )
    print("Preparing the dataset with feature engineering")
    return features_pipeline.fit_transform(dataset, target_y)


def data_train_test_split(X, y):
    """
    Split dataset to train/test samples
    :param X:
    :param y:
    :return:
    """
    log_y = np.log1p(y)

    X_train, X_test, y_train, y_test = train_test_split(X, log_y,
                                                        test_size=0.25,  # Proportion of the test dataset
                                                        random_state=64,
                                                        # Fix the state of random generator, to get repeatitive results each time
                                                        shuffle=True,
                                                        # Randomly pick test dataset to maintain original distribution
                                                        )
    return X_train, X_test, y_train, y_test