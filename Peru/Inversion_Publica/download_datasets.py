# Importar librerias

import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd

# Configuraciones

# warnings.filterwarnings("ignore")
# urllib3.disable_warnings(
#     urllib3.exceptions.InsecureRequestWarning
# )

load_dotenv(dotenv_path='.env')

# Establecer fecha de corte

corte = 20241231
repo = 'Peru/Inversion_Publica/datasets'

# Importar y guardar inversiones activas

activas = pd.read_csv(
    os.getenv('url_detalle_inv')
)

activas.to_csv(
    f'{repo}/DETALLE_INVERSIONES_{corte}.csv',
    index=False
)

# Importar y guardar inversiones cerradas

cerradas = pd.read_csv(
    os.getenv('url_cierre_inv')
)

cerradas.to_csv(
    f'{repo}/CIERRE_INVERSIONES_{corte}.csv',
    index=False
)

# Importar y guardar inversiones desactivadas

desactivadas = pd.read_csv(
    os.getenv('url_inv_desactivadas')
)

desactivadas.to_csv(
    f'{repo}/INVERSIONES_DESACTIVADAS_{corte}.csv',
    index=False
)

# Importar datasets guardados

# activas = pd.read_csv(
#     f'{repo}/DETALLE_INVERSIONES_{corte}.csv'
# )
#
# cerradas = pd.read_csv(
#     f'{repo}/CIERRE_INVERSIONES_{corte}.csv'
# )
#
# desactivadas = pd.read_csv(
#     f'{repo}/INVERSIONES_DESACTIVADAS_{corte}.csv'
# )

# Unir datasets

activas = (
    activas
    .assign(
        DEVENGADO_ACUMULADO = activas['DEV_ANIO_ACTUAL'] + activas['DEVEN_ACUMUL_ANIO_ANT']
    )
    .rename(
        {'NOMBRE_OPMI': 'NOM_OPMI',
         'NOMBRE_UF': 'NOM_UF',
         'NOMBRE_UEI': 'NOM_UEI',
         'NOMBRE_UEP': 'NOM_UEP'},
        axis=1
    )
)

cerradas = cerradas.rename(
    {'DEVEN_ACUMULADO': 'DEVENGADO_ACUMULADO'},
    axis=1
)

desactivadas = desactivadas.rename(
    {'COD_SNIP': 'CODIGO_SNIP'},
    axis=1
)

inv = (
    pd.concat(
        [activas, cerradas, desactivadas],
        axis=0,
        ignore_index=True
    )
    .drop_duplicates(subset='CODIGO_SNIP')
    .reset_index(drop=True)
)

# Completar con ceros los valores nulos en columnas numericas

for column in inv.columns:
    if pd.api.types.is_numeric_dtype(inv[column]):
        inv[column] = inv[column].fillna(0)

# Convertir fechas a formato de fechas

fechas = ['FECHA_REGISTRO',
          'FECHA_VIABILIDAD',
          'FEC_REG_F9',
          'FECHA_ULT_ACT_F12B',
          'ULT_FEC_DECLA_ESTIM',
          'FEC_INI_EJECUCION',
          'FEC_FIN_EJECUCION',
          'FEC_INI_EJEC_FISICA',
          'FEC_FIN_EJEC_FISICA',
          'FEC_CIERRE',
          'FEC_INI_OPER']

for fecha in fechas:

    inv[fecha] = inv[fecha].str.extract(
        r'(\d{4}-\d{2}-\d{2})',
        expand=False
    )

    inv[fecha] = pd.to_datetime(
        inv[fecha],
        format='%Y-%m-%d',
        errors='coerce'
    )

# Transformar columnas

inv = (
    inv
    .assign(
        SALDO = np.where(
            inv['DEVENGADO_ACUMULADO'] > inv['COSTO_ACTUALIZADO'],
            0,
            inv['COSTO_ACTUALIZADO'] - inv['DEVENGADO_ACUMULADO']
        ),
        EJECUCION = np.where(
            inv['COSTO_ACTUALIZADO'] == 0,
            0,
            inv['DEVENGADO_ACUMULADO'] / inv['COSTO_ACTUALIZADO']
        ),
        COSTO_ACTUALIZADO = inv['COSTO_ACTUALIZADO'] - inv['CTRL_CONCURR'],
        ANIO_REGISTRO = inv['FECHA_REGISTRO'].dt.year,
        ANIO_VIABILIDAD = inv['FECHA_VIABILIDAD'].dt.year
    )
    .replace(
        {
            'ESTADO': {
                'DESACTIVADO PERMANENTE': 'DESACTIVADO',
                'DESACTIVADO ': 'DESACTIVADO',
                'DESACTIVADO TEMPORAL': 'DESACTIVADO',
                'DESACTIVADO   PERMANENTE': 'DESACTIVADO'
            },
            'SITUACION': {
                'VIABLE': 'VIABLE/APROBADO',
                'APROBADO': 'VIABLE/APROBADO',
                'NO VIABLE': 'NO VIABLE/APROBADO',
                'NO APROBADO': 'NO VIABLE/APROBADO'
            },
            'DEPARTAMENTO': {
                '-MUL.DEP-': '_MULTIDEPARTAMENTO_'
            }
        }
    )
    .fillna({'DEPARTAMENTO':'_NO REGISTRA_'})
)

# Categorizar variables

inv['ESTADO'] = pd.Categorical(
    inv['ESTADO'],
    categories=['ACTIVO', 'CERRADO', 'DESACTIVADO']
)

inv['SITUACION'] = pd.Categorical(
    inv['SITUACION'],
    categories=['VIABLE/APROBADO', 'NO VIABLE/APROBADO', 'EN FORMULACION']
)

inv['NIVEL'] = pd.Categorical(
    inv['NIVEL'],
    categories=['GN', 'GR', 'GL']
)

# Exportar

inv.to_parquet(f'{repo}/inversiones_odmef_{corte}.parquet', engine='pyarrow')






