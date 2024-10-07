import streamlit as st
import pandas as pd
from analysis import get_prescriptions_data, get_meds_data
from concurrent.futures import ThreadPoolExecutor
from st_aggrid import AgGrid

# Add progress spinner while loading data
with st.spinner("Fetching data..."):
    df = get_prescriptions_data()
    meds = get_meds_data()

# Lab selection dropdown
lab_options = meds['lab'].unique()
selected_lab = st.selectbox('Select a Laboratory', lab_options)

# Filter medications based on selected lab
meds_filtered = meds[meds.idvmp.isin(meds[meds.lab == selected_lab].idvmp.unique())].idmedicamento.unique()
meds_rigido_filtered = meds[meds.lab == selected_lab].idmedicamento.unique()

# Convert date column to datetime and extract month
df.datamodificacao = pd.to_datetime(df.datamodificacao)
df['mes'] = df.datamodificacao.dt.month

# Function to format numbers in Brazilian style
def format_brazilian_style(number):
    if pd.isna(number):
        return ""
    return f"{int(number):,}".replace(",", "TEMP").replace(".", ",").replace("TEMP", ".")

def format_brazilian_percentage_style(number):
    if pd.isna(number):
        return ""
    return f"{number * 100:,.1f}%".replace(",", "TEMP").replace(".", ",").replace("TEMP", ".")

# Show analysis results dynamically
st.write(f'**Volume de prescrições com medicamentos elegíveis do laboratório {selected_lab}:**')
volume_data = df[df.idmedicamento.isin(meds_filtered)].groupby('mes').idprescricaomedicamento.nunique().reset_index()
volume_data['idprescricaomedicamento'] = volume_data['idprescricaomedicamento'].apply(format_brazilian_style)
st.dataframe(volume_data)

tipomed_df = meds[['idmedicamento', 'tipomed']].drop_duplicates(subset='idmedicamento')
df = df.merge(tipomed_df, on='idmedicamento', how='inner')

st.write('**Desse volume elegível, quanto é genérico e quanto é marca?**')
analysis2 = df[df.idmedicamento.isin(meds_filtered)].groupby('tipomed').idprescricaomedicamento.nunique().reset_index()
analysis2['idprescricaomedicamento'] = analysis2['idprescricaomedicamento'].apply(format_brazilian_style)
st.dataframe(analysis2)

# Analysis 3: Most prescribed medications for the selected lab
st.write(f'**Quais são os medicamentos que o laboratório {selected_lab} pode atender mais prescritos?**')
analysis3 = df[df.idmedicamento.isin(meds_rigido_filtered)].groupby('nome').idprescricaomedicamento.nunique().reset_index().sort_values(by='idprescricaomedicamento', ascending=False).head(50)
analysis3['idprescricaomedicamento'] = analysis3['idprescricaomedicamento'].apply(format_brazilian_style)
st.dataframe(analysis3)

# Share of prescriptions calculation
st.write('**Share de prescrições**')
df = df.merge(meds[['idmedicamento', 'idvmp', 'lab']].drop_duplicates(), on='idmedicamento', how='left')

# Retrieve medications from selected laboratory and competitors
meds_ref = meds[(meds.idvmp.isin(meds[(meds.lab == selected_lab) & (meds.tipomed != 'GENÉRICO')].idvmp.unique()))].idmedicamento.unique()

# Group by idvmp, name, and lab to calculate the share of prescriptions
res = df[df.idmedicamento.isin(meds_ref)].groupby(['idvmp', 'nome', 'lab']).idprescricaomedicamento.nunique().reset_index()

# Create an output DataFrame to summarize findings
output = pd.DataFrame()
idvmp_lista = []
total_prescricoes_lista = []
meds_lista = []

share_laboratorio_lista = []
med_concorrente_lista = []
share_concorrente_lista = []
rank_meds_list = []

# Function to calculate share for each idvmp
def calculate_share(idvmp, res, selected_lab):
    tmp = res[res.idvmp == idvmp].sort_values(by='idprescricaomedicamento', ascending=False)
    total = tmp.idprescricaomedicamento.sum()
    
    meds = ', '.join(list(tmp[tmp.lab == selected_lab].nome))
    presc_laboratorio = tmp[tmp.lab == selected_lab].idprescricaomedicamento.sum()
    share_laboratorio = presc_laboratorio / total if total > 0 else 0

    med_concorrente = list(tmp.nome)[0]
    presc_concorrente = list(tmp.idprescricaomedicamento)[0]
    share_concorrente = presc_concorrente / total if total > 0 else 0

    rank_meds = ', '.join(list(tmp.nome))

    return idvmp, total, meds, share_laboratorio, med_concorrente, share_concorrente, rank_meds

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor() as executor:
    results = list(executor.map(lambda idvmp: calculate_share(idvmp, res, selected_lab), res.idvmp.unique()))

# Convert results to DataFrame
output = pd.DataFrame(results, columns=['idVMP', 'total prescricoes', 'Medicamento Laboratorio', 'Share Laboratorio', 'Med Concorrente Lider', 'Share Concorrente Lider', 'Rank Medicamentos'])

# Format the relevant numeric columns for output
output['total prescricoes'] = output['total prescricoes'].apply(format_brazilian_style)

# Convert to percentage format and apply Brazilian style
output['Share Laboratorio'] = output['Share Laboratorio'].apply(format_brazilian_percentage_style)
output['Share Concorrente Lider'] = output['Share Concorrente Lider'].apply(format_brazilian_percentage_style)

# Display the final output with pagination
AgGrid(output, enable_enterprise_modules=True, fit_columns_on_grid_load=True, pagination=True, paginationPageSize=10)