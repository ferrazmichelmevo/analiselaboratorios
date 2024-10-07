from athena_handler import run_query
import pandas as pd
import streamlit as st

@st.cache_data
def get_prescriptions_data():
    query = """
    select p.idprescricao, p.datamodificacao, pm.* 
    from prescricao p 
    inner join prescricao_medicamento pm on p.idprescricao = pm.idprescricao
    where p.year='2024' and p.month in ('09','10')
    """
    return run_query(query, 'powerbi_production_mktplace')

@st.cache_data
def get_meds_data():
    query = """
    SELECT idmedicamento, idapresentacao, ean, produto, nome_completo, idvmp, lab, tipomed, idcategoriacomercial
    FROM base_medicamentos
    """
    records = run_query(query, 'vtex_silver')
    meds = pd.DataFrame(records)
    
    meds.loc[meds['tipomed'] == 'GENÉRICO', 'lab'] = '-'

    return meds
