# Importar librerías

import matplotlib.ticker as mticker
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import textwrap
import numpy as np

# Importar dataset

inv = pd.read_parquet('Peru/Inversion_Publica/datasets/inversiones_odmef_20241231.parquet')

# Paletas de colores

colors = ['#86C7D9',
           '#D9D1A7',
           '#A7D9D1',
           '#D1A786',
           '#5F9EA0',
           '#D2B48C',
           '#C0C0C0',
           '#708090',
           '#F4A460',
           '#CD853F',
           '#556B2F',
           '#2F4F4F',
           '#8B4513',
           '#BDB76B',
           '#7B68EE',
           '#4682B4',
           '#A0522D',
           '#32CD32',
           '#FF8C00',
           '#8A2BE2',
           '#FA8072',
           '#008080',
           '#DC143C',
           '#20B2AA',
           '#FF4500',
           '#800000',
           '#CD5C5C',
           '#00CED1',
           '#8B008B']

# Gráfico de sectores

def pie(labels,
        sizes,
        name=None,
        figsize=(10,10),
        palette=None,
        text_size=20,
        per_size=20):
    sns.set(style="whitegrid")

    # Crear la figura con márgenes reducidos y tamaño específico
    fig, ax = plt.subplots(figsize=figsize, subplot_kw=dict(aspect="equal"))

    # Configurar el gráfico circular con la paleta de colores personalizada
    wedges, texts, autotexts = ax.pie(sizes,
                                      labels=labels,
                                      autopct='%1.1f%%',
                                      startangle=140,
                                      pctdistance=0.85,
                                      colors=palette)

    # Ajustar el tamaño de la fuente de las etiquetas y porcentajes
    for text, autotext in zip(texts, autotexts):
        text.set_size(text_size)  # Tamaño de la fuente para las etiquetas
        autotext.set_size(per_size)  # Tamaño de la fuente para los porcentajes

    # Reducir los márgenes
    plt.subplots_adjust(left=0, right=0.8, top=0.9, bottom=0.1)

    # Guardar la figura con alta resolución
    if name:
        plt.savefig(name, dpi=300, bbox_inches='tight', pad_inches=0)
    return plt.show()


# Gráfico de líneas

def vline(labels,
          sizes_list,
          names=None,
          figsize=(20, 10),
          palette=None,
          title='',
          xlabel='',
          ylabel='',
          box=None,
          fontsize=12,
          save='fig'):
    # Establecer el tamaño de fuente predeterminado
    plt.rc('font', size=fontsize)

    # Crear el gráfico de líneas
    fig, ax = plt.subplots(figsize=figsize)  # Ajustar el tamaño de la figura

    # Agregar título y etiquetas a los ejes
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    # Tramar los puntos con valores numéricos en el eje x
    x = np.arange(len(labels))

    # Verificar si se proporcionaron nombres
    if names is None:
        names = [f'Series {i + 1}' for i in range(len(sizes_list))]

    # Graficar cada serie en sizes_list
    for i, sizes in enumerate(sizes_list):
        label = names[i]
        ax.plot(x, sizes, marker='o', linestyle='-', label=label)

    # Establecer las etiquetas del eje x como las etiquetas proporcionadas
    ax.set_xticks(x)
    ax.set_xticklabels(labels)

    # Quitar bordes del gráfico
    plt.box(on=box)

    # Formatear los ticks del eje y sin decimales y con coma como separador de miles
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

    # Agregar la grilla
    ax.grid(True)

    # Añadir las líneas de los ejes
    ax.axhline(y=0, color='black', linewidth=1.3)
    ax.axvline(x=0, color='black', linewidth=1.3)

    # Agregar leyenda
    ax.legend(loc='upper left')

    # Guardar el gráfico como un archivo PNG en alta resolución en el directorio de trabajo
    if save:
        plt.savefig(save, dpi=1200, bbox_inches='tight')

    return plt.show()


# Estimar inversiones por estado

table_estados = pd.pivot_table(
    inv,
    values='CODIGO_SNIP',
    index='ESTADO',
    aggfunc='count'
).reset_index()

# Estimar inversiones por anio

table_anios = pd.pivot_table(
    inv[~inv['ANIO_VIABILIDAD'].isin([1999,2025])],
    values='CODIGO_SNIP',
    index='ANIO_VIABILIDAD',
    aggfunc='count'
).reset_index()

vline(table_anios['ANIO_VIABILIDAD'].astype(int),
      [table_anios['CODIGO_SNIP']]
      )

pie(table_estados['ESTADO'],
        table_estados['CODIGO_SNIP'],
        name=None,
        figsize=(10,10),
        palette=colors,
        text_size=20,
        per_size=20)

