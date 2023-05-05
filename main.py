import plotly.graph_objects as go
from dash import dcc, html, Output, Input, dash_table, dash
import cx_Oracle
import pandas as pd
from datetime import datetime

dsn = cx_Oracle.makedsn("10.40.3.10", 1521, service_name="f3ipro")
connection = cx_Oracle.connect(user=r"focco_consulta", password=r'consulta3i08', dsn=dsn, encoding="UTF-8")
cur = connection.cursor()
cur.execute(
    r"SELECT  TMOV.TEMPO,TOR.NUM_ORDEM, TOR.QTDE, SUM(ENG.PESO_LIQ * TMOV.QUANTIDADE), TMAQ.DESCRICAO, TOP.DESCRICAO, TFUN.NOME, EXTRACT(MONTH FROM TMOV.DT_APONT) "
    r"FROM FOCCO3I.TORDENS_MOVTO TMOV "
    r",FOCCO3I.TFUNCIONARIOS TFUN "
    r",FOCCO3I.TORDENS_ROT TROT "
    r",FOCCO3I.TOPERACAO TOP "
    r",FOCCO3I.TORDENS TOR "
    r",FOCCO3I.TORD_ROT_FAB_MAQ TFAB "
    r",FOCCO3I.TMAQUINAS TMAQ "
    r",FOCCO3I.TITENS_ENGENHARIA ENG "
    r",FOCCO3I.TITENS_EMPR EMP "
    r",FOCCO3I.TITENS_PLANEJAMENTO TPL "
    r"WHERE TMOV.FUNC_ID = TFUN.ID "
    r"AND TMOV.TORDEN_ROT_ID = TROT.ID "
    r"AND TFAB.TORDEN_ROT_ID = TROT.ID "
    r"AND TMAQ.ID = TFAB.MAQUINA_ID "
    r"AND TROT.ORDEM_ID = TOR.ID "
    r"AND TROT.OPERACAO_ID = TOP.ID "
    r"AND TFAB.TORDEN_ROT_ID = TROT.ID "
    r"AND EMP.ID = ENG.ITEMPR_ID "
    r"AND TOR.ITPL_ID = TPL.ID "
    r"AND TPL.ITEMPR_ID = EMP.ID "
    r"AND TFUN.NOME NOT LIKE ('%AVILA%') AND TFUN.NOME NOT LIKE ('%PANA%') "
    r"AND TMAQ.DESCRICAO IN ('CALANDRA','DOBRADEIRA', 'FICEP', 'FURADEIRA','GUILHOTINA','JATO','LIXADEIRA','METALEIRA', 'MIG', 'PARAFUSADEIRA', 'PISTOLA', 'PLASMA', 'PRENSA', 'PUNCIONADEIRA', 'SERRA-FITA', 'TANQUE', 'TORNO')   "
    r"AND TMOV.DT_APONT BETWEEN TO_DATE ('01/01/2023', 'DD/MM/YYYY') AND TO_DATE (SYSDATE) "
    r"GROUP BY TMOV.TEMPO,TOR.NUM_ORDEM, TOR.QTDE, TMAQ.DESCRICAO, TOP.DESCRICAO, TFUN.NOME, EXTRACT(MONTH FROM TMOV.DT_APONT) "
)
df = cur.fetchall()
df = pd.DataFrame(df, columns=['TEMPO', 'NUM_ORDEM', 'QTDE', 'PESO', 'MAQUINA', 'OPERACAO', 'FUNC', 'DT_APONT'])
df = df.groupby(['MAQUINA', 'FUNC', 'OPERACAO', 'DT_APONT']).agg(
    {'TEMPO': ['sum'], 'NUM_ORDEM': ['count'], 'PESO': ['sum'], 'QTDE': ['sum']})
df.columns = ['TEMPO', 'NUM_ORDEM', 'PESO', 'QTDE']
df['TEMPO'] = df['TEMPO'] / 60
df = df.reset_index().round(2)

currentMonth = datetime.now().month
app = dash.Dash(__name__)
PAGE_SIZE = 4

app.layout = html.Div([
    html.Label("MAQUINA:", style={'fontSize': 16, 'textAlign': 'center'}),
    dcc.Dropdown(
        id='dropmaquina',
        options=[{'label': s, 'value': s} for s in sorted(df['MAQUINA'].unique())],
        value='GUILHOTINA',
        clearable=False
    ),
    html.Hr(),
    html.Label("OPERAÇÃO:", style={'fontSize': 16, 'textAlign': 'center'}),
    dcc.Dropdown(id='dropoperacao', options=[], multi=True),
    html.Hr(),
    dcc.Graph(id='graph', figure={}),
    html.Hr(),
    dcc.Slider(1, 12, 1, marks={1: {'label': 'JAN'},
                                2: {'label': 'FEV'},
                                3: {'label': 'MAR'},
                                4: {'label': 'ABR'},
                                5: {'label': 'MAI'},
                                6: {'label': 'JUN'},
                                7: {'label': 'JUL'},
                                8: {'label': 'AGO'},
                                9: {'label': 'SET'},
                                10: {'label': 'OUT'},
                                11: {'label': 'NOV'},
                                12: {'label': 'DEZ'},
                                },
               id='meu_slider',
               value=currentMonth - 1,
               included=False
               ),

    html.Hr(),
    dash_table.DataTable(
        id='datatable-paging',
        page_current=0,
        page_size=PAGE_SIZE,
        page_action='custom'
    )
])


@app.callback(
    Output('dropoperacao', 'options'),
    Input('dropmaquina', 'value'))
def maquinas(chosen_state):
    dff = df[df['MAQUINA'] == chosen_state]
    return [{'label': c, 'value': c} for c in sorted(dff['OPERACAO'].unique())]


@app.callback(
    Output('dropoperacao', 'value'),
    Input('dropoperacao', 'options'))
def operacao(available_options):
    return [x['value'] for x in available_options]


@app.callback(
    Output('graph', 'figure'),
    Input('dropoperacao', 'value'),
    Input('dropmaquina', 'value'),
    Input('meu_slider', 'value'))
def update_grpah(selected_counties, selected_state, mes):
    if len(selected_counties) == 0:
        return dash.no_update
    else:
        dff = df[(df['MAQUINA'] == selected_state) & (df['OPERACAO'].isin(selected_counties)) & (df['DT_APONT'] == mes)]
        dff['TPERCENT'] = (dff['TEMPO'] / dff['TEMPO'].sum()) * 100
        dff['QPERCENT'] = (dff['QTDE'] / dff['QTDE'].sum()) * 100
        dff['PPERCENT'] = (dff['PESO'] / dff['PESO'].sum()) * 100
        dff['NPERCENT'] = (dff['NUM_ORDEM'] / dff['NUM_ORDEM'].sum()) * 100

        fig = go.Figure(data=[
            go.Bar(name='Ordens',
                   y=dff['FUNC'],
                   x=dff['NUM_ORDEM'],
                   text=['{} - {:.0%}'.format(v, p / 100) for v, p in zip(dff['NUM_ORDEM'], dff['NPERCENT'])],
                   orientation='h'
                   ),
            go.Bar(name='Peso (Kg)',
                   y=dff['FUNC'],
                   x=dff['PESO'],
                   text=['{} - {:.0%}'.format(v, p / 100) for v, p in zip(dff['PESO'], dff['PPERCENT'])],
                   orientation='h'
                   ),
            go.Bar(name='Peças',
                   y=dff['FUNC'],
                   x=dff['QTDE'],
                   text=['{} - {:.0%}'.format(v, p / 100) for v, p in zip(dff['QTDE'], dff['QPERCENT'])],
                   orientation='h'
                   ),
            go.Bar(name='Tempo (Hora)',
                   y=dff['FUNC'],
                   x=dff['TEMPO'],
                   text=['{} - {:.0%}'.format(v, p / 100) for v, p in zip(dff['TEMPO'], dff['TPERCENT'])],
                   orientation='h'
                   ),
        ])
        fig.update_layout(
            barmode='group',
            yaxis={'categoryorder': 'total ascending'},
            width=1360,
            height=650
        ),
        fig.update_traces(
            textfont_size=12,
            textangle=0,
            textposition="outside")
        return fig


@app.callback(
    Output('datatable-paging', 'data'),
    Input('dropmaquina', 'value'),
    Input('dropoperacao', 'value'),
    Input('meu_slider', 'value')
)
def update_table(maq, op, mes):
    if len(op) == 0:
        return dash.no_update
    else:
        dftt = df[(df['DT_APONT'] == mes) & (df['MAQUINA'] == maq)]
        dfta = dftt[['NUM_ORDEM', 'PESO', 'QTDE', 'TEMPO']].copy()
        dfta = dfta.agg({'NUM_ORDEM': ['sum'], 'PESO': ['sum'], 'QTDE': ['sum'], 'TEMPO': ['sum']}).round(2)
        return dfta.to_dict('records')


if __name__ == '__main__':
    app.run(host='10.40.3.48', port=8060)
