from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from pymongo import MongoClient
import pandas as pd
import os

client = MongoClient(f'mongodb+srv://karthiknaik122003:ZCDbyXnnLZTzM3U@cluster0.olacnku.mongodb.net/')

db = client['supplierData']

collection = db['supplierData']

documents = collection.find()
df = pd.DataFrame(list(documents))
# df = pd.read_csv('C:\Local Disk F\ML\Easework AI\Supplier Discovery\Database\Suppliers Data 4 1.csv')

# Define FastAPI app
app = FastAPI()

# Models for response
class SupplierStats(BaseModel):
    total_suppliers: int
    preferred_suppliers: int
    catalog_suppliers: int
    contract_suppliers: int
    total_suppliers_list: list
    preferred_suppliers_list: list
    catalog_suppliers_list: list
    contract_suppliers_list: list
    
class CategoriesSpend(BaseModel):
    supp_c1_spend: int
    supp_c2_spend: int
    supp_c3_spend: int
    cat1_df: list
    cat2_df: list
    cat3_df: list
    
class SuppliersbyLocation(BaseModel):
    country1_sup: int
    country2_sup: int
    country3_sup: int
    country1_df: list
    country2_df: list
    country3_df: list

class SuppliersbyFinancialScore(BaseModel):
    distinct_high_fs: int
    distinct_medium_fs: int
    distinct_low_fs: int
    fs1_df: list
    fs2_df: list
    fs3_df: list
    
class SuppliersbyProductReview(BaseModel):
    distinct_high_pr: int
    distinct_medium_pr: int
    distinct_low_pr: int
    prodreview1: list
    prodreview2: list
    prodreview3: list
    
class SuppliersbyRegulatoryScore(BaseModel):
    distinct_high_rs: int
    distinct_medium_rs: int
    distinct_low_int: int
    regulatory_score1: list
    regulatory_score2: list
    regulatory_score3: list
    
class SuppliersbyRiskScore(BaseModel):    
    distinct_high_risk: int
    distinct_meedium_risk: int
    distinct_low_risk: int
    supplier_risk_1: list
    supplier_risk_2: list
    supplier_risk_3: list


@app.get("/stats", response_model=SupplierStats)
def get_supplier_stats():

    # Total Suppliers
    total_suppliers = (df['SupplierID'].nunique())
    total_suppliers_list = df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()

    # Preferred Suppliers
    PO_df = df[df['PONumber'] != "Not Applicable"]
    preferred_suppliers = (PO_df['SupplierID'].nunique())
    
    preferred_suppliers_list = PO_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    
    # Contact Suppliers
    contract_df = df[df['Contract ID'] != "Not Applicable"]
    contract_suppliers = (contract_df['SupplierID'].nunique())
    contract_suppliers_list = contract_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    
    # Catalog Suppliers
    catalog_df = df[df['Catalog ID'] != "Not Applicable"]
    catalog_suppliers = (catalog_df['SupplierID'].nunique())
    catalog_suppliers_list = catalog_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    
    return SupplierStats(
        total_suppliers=total_suppliers,
        preferred_suppliers=preferred_suppliers,
        catalog_suppliers=catalog_suppliers,
        contract_suppliers=contract_suppliers,
        total_suppliers_list=total_suppliers_list,
        preferred_suppliers_list=preferred_suppliers_list,
        contract_suppliers_list=contract_suppliers_list,
        catalog_suppliers_list=catalog_suppliers_list
    )
    
    
    
    
@app.get("/cat", response_model=CategoriesSpend)
def get_categories_spend():
    PO_df = df[df['PONumber'] != "Not Applicable"]
    PO_df['TotalCost'] = pd.to_numeric(PO_df['TotalCost'], errors='coerce')
    
    # Categories Spend
    categories_spend = PO_df.groupby('Category').agg(TotalSpend=('TotalCost', 'sum'), SupplierCount=('SupplierID', 'nunique')).reset_index()
    categories_spend = categories_spend.sort_values(by = 'TotalSpend', ascending = False)
    supp_c1_spend = int(categories_spend['SupplierCount'][0])
    supp_c2_spend = int(categories_spend['SupplierCount'][1])
    supp_c3_spend = int(categories_spend['SupplierCount'][2])
    
    cat1_df = df[df['Category'] == categories_spend['Category'][0]]
    cat1_df = cat1_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    cat2_df = df[df['Category'] == categories_spend['Category'][1]]
    cat2_df = cat2_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    cat3_df = df[df['Category'] == categories_spend['Category'][2]]
    cat3_df = cat3_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    
    
    
    return CategoriesSpend(
        supp_c1_spend = supp_c1_spend,
        supp_c2_spend = supp_c2_spend,
        supp_c3_spend = supp_c3_spend,
        cat1_df=cat1_df,
        cat2_df=cat2_df,
        cat3_df=cat3_df
    )
    
    
    
    
@app.get("/loc", response_model=SuppliersbyLocation)
def get_categories_spend():
    # Supplier by Location (Country)
    PO_df = df[df['PONumber'] != "Not Applicable"]
    PO_df['TotalCost'] = pd.to_numeric(PO_df['TotalCost'], errors='coerce')
    suppliers_by_location = PO_df.groupby('Country').agg(
    totalSpend = ('TotalCost', 'sum'), Suppliers_count = ('SupplierID', 'nunique')).reset_index()
    suppliers_by_location = suppliers_by_location.sort_values(by = 'totalSpend', ascending = False)
    country1_sup = int(suppliers_by_location['totalSpend'][0])
    country2_sup = int(suppliers_by_location['totalSpend'][1])
    country3_sup = int(suppliers_by_location['totalSpend'][2])
    
    country1_df = df[df['Country'] == suppliers_by_location['Country'][0]]
    country1_df = country1_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    country2_df = df[df['Country'] == suppliers_by_location['Country'][1]]
    country2_df = country2_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    country3_df = df[df['Country'] == suppliers_by_location['Country'][2]]
    country3_df = country3_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    
    return SuppliersbyLocation(
        country1_sup=country1_sup,
        country2_sup=country2_sup,
        country3_sup=country3_sup,
        country1_df = country1_df,
        country2_df = country2_df,
        country3_df = country3_df
    )
    
    
    
    
@app.get("/finsc", response_model = SuppliersbyFinancialScore)
def get_financial_scores():
    PO_df = df[df['PONumber'] != "Not Applicable"]
    PO_df['TotalCost'] = pd.to_numeric(PO_df['TotalCost'], errors='coerce')
    distinct_high_fs = df[(df['Financial Score'] >= 70) & (df['Financial Score'] <= 90)]['Supplier Name'].nunique()
    distinct_medium_fs = df[(df['Financial Score'] >= 50) & (df['Financial Score'] < 70)]['Supplier Name'].nunique()
    distinct_low_fs = df[(df['Financial Score'] >= 30) & (df['Financial Score'] < 50)]['Supplier Name'].nunique()
    
    fs1_df = df[(df['Financial Score'] >=70) & (df['Financial Score'] <= 90)]
    fs1_df = fs1_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    fs2_df = df[(df['Financial Score'] >=50) & (df['Financial Score'] < 70)]
    fs2_df = fs2_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    fs3_df = df[(df['Financial Score'] >=30) & (df['Financial Score'] < 50)]
    fs3_df = fs3_df[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    
    return SuppliersbyFinancialScore(
        distinct_high_fs=distinct_high_fs,
        distinct_medium_fs=distinct_medium_fs,
        distinct_low_fs=distinct_low_fs,
        fs1_df = fs1_df,
        fs2_df = fs2_df,
        fs3_df = fs3_df
    )




@app.get("/prodrev", response_model = SuppliersbyProductReview)
def get_productreview_scores():
    # Suppliers by Product Review Score
    unique_df = df.drop_duplicates(subset=['Supplier Name', 'Product Review Score'])
    distinct_high_pr = unique_df[(unique_df['Product Review Score'] >= 70) & (unique_df['Product Review Score'] <= 90)]['Supplier Name'].nunique()
    distinct_medium_pr = unique_df[(unique_df['Product Review Score'] >= 50) & (unique_df['Product Review Score'] < 70)]['Supplier Name'].nunique()
    distinct_low_pr = unique_df[(unique_df['Product Review Score'] >= 20) & (unique_df['Product Review Score'] < 50)]['Supplier Name'].nunique()
    
    productreview1 = df[(df['Product Review Score'] >=70) & (df['Product Review Score'] <= 90)]
    productreview1 = productreview1[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    productreview2 = df[(df['Product Review Score'] >=50) & (df['Product Review Score'] < 70)]
    productreview2 = productreview2[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    productreview3 = df[(df['Product Review Score'] >=30) & (df['Product Review Score'] < 50)]
    productreview3 = productreview3[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    
    return SuppliersbyProductReview(
        distinct_high_pr=distinct_high_pr,
        distinct_medium_pr=distinct_medium_pr,
        distinct_low_pr=distinct_low_pr,
        prodreview1=productreview1,
        prodreview2=productreview2,
        prodreview3=productreview3
    )
    

    
@app.get("/regsc", response_model = SuppliersbyRegulatoryScore)
def get_regulatory_scores():    
    # Suppliers by Regulatory Score
    distinct_high_rs = df[(df['Regulatory Score'] >= 70) & (df['Regulatory Score'] <= 95)]['Supplier Name'].nunique()
    distinct_medium_rs = df[(df['Regulatory Score'] >= 50) & (df['Regulatory Score'] < 70)]['Supplier Name'].nunique()
    distinct_low_rs = df[(df['Regulatory Score'] >= 20) & (df['Regulatory Score'] < 50)]['Supplier Name'].nunique()
    
    regulatory_score1 = df[(df['Regulatory Score'] >=70) & (df['Regulatory Score'] <= 90)]
    regulatory_score1 = (regulatory_score1[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID'])).values.tolist()
    regulatory_score2 = df[(df['Regulatory Score'] >=50) & (df['Regulatory Score'] < 70)]
    regulatory_score2 = (regulatory_score2[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID'])).values.tolist()
    regulatory_score3 = df[(df['Regulatory Score'] >=30) & (df['Regulatory Score'] < 50)]
    regulatory_score3 = (regulatory_score3[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID'])).values.tolist()
    
    return SuppliersbyRegulatoryScore(
        distinct_high_rs=distinct_high_rs,
        distinct_medium_rs=distinct_medium_rs,
        distinct_low_int=distinct_low_rs,
        regulatory_score1=regulatory_score1,
        regulatory_score2=regulatory_score2,
        regulatory_score3=regulatory_score3
    )
    
    
    
@app.get("/risk", response_model = SuppliersbyRiskScore)
def get_risk_scores(): 
    # Suppliers by Risk Level
    distinct_high_risk = df[df['Risk Level'] == 'High']['Supplier Name'].nunique()
    distinct_medium_risk = df[df['Risk Level'] == 'Medium']['Supplier Name'].nunique()
    distinct_low_risk = df[df['Risk Level'] == 'Low']['Supplier Name'].nunique()

    supplier_risk_1 = df[df['Risk Level'] == 'High']
    supplier_risk_1 = supplier_risk_1[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    supplier_risk_2 = df[df['Risk Level'] == 'Medium']
    supplier_risk_2 = supplier_risk_2[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()
    supplier_risk_3 = df[df['Risk Level'] == 'Low']
    supplier_risk_3 = supplier_risk_3[['SupplierID', 'Supplier Name', 'Supplier Industry', 'Financial Score', 'Risk Level', 'ContactNumber', 'Email']].drop_duplicates(subset=['SupplierID']).values.tolist()

    return SuppliersbyRiskScore(
        distinct_high_risk=distinct_high_risk,
        distinct_meedium_risk=distinct_medium_risk,
        distinct_low_risk=distinct_low_risk,
        supplier_risk_1=supplier_risk_1,
        supplier_risk_2=supplier_risk_2,
        supplier_risk_3=supplier_risk_3
    )


# # To run the server use:
# # uvicorn main:app --reload