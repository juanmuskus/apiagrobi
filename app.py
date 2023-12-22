import datetime
import pandas as pd
import pmdarima as pm
from flask import Flask, request, jsonify
import warnings
import mysql.connector
from flask_cors import CORS
warnings.filterwarnings("ignore")
app = Flask(__name__)
CORS(app)
class Insumo:
    def __init__(self, departamento_id, municipio_id, producto_id, cantidad_prediccion, frecuencia):
        self.departamento_id = departamento_id
        self.municipio_id = municipio_id
        self.producto_id = producto_id
        self.cantidad_prediccion = cantidad_prediccion
        self.frecuencia = frecuencia
def conexion():
    db_username = 'admin_vista'
    db_password = 'Unicor@2023'
    db_host = '74.48.111.45'
    db_name = 'insumos'
    return mysql.connector.connect( user=db_username,
                                    password=db_password,
                                    host=db_host,
                                    database=db_name,
                                    auth_plugin='mysql_native_password')
@app.route('/')
def home():
    return {'Status': 'Running in datetime: '+str(datetime.datetime.now())}
@app.route('/insumos', methods=['POST'])
def prediccion_insumo():
    TiempoInicio = datetime.datetime.now()
    conn = conexion()
    data = request.get_json()
    insumo = Insumo(**data)
    df = pd.DataFrame()
    try:
        sql_query = "select * from vista_precios where departamento_id = %s and municipio_id =%s and producto_id = %s" % (
            insumo.departamento_id, insumo.municipio_id, insumo.producto_id)
        df = pd.read_sql(sql_query,conn)
        if len(df)==0:
            return jsonify({'Estado':'Error', 'Data':{'Mensaje':'No hay datos para realizar la prediccion'}})
        df.set_index('fechapublicacion', inplace=True)
        df = df['valor']
        df = df.resample(insumo.frecuencia).mean()
        model = pm.auto_arima(df)
        pred = model.predict(n_periods=insumo.cantidad_prediccion)
        pred = pred.astype(int)
        pred = pred.reset_index(drop=True)
        min_val = str(df.astype(int).min())
        max_val = str(df.astype(int).max())

        time_difference = datetime.datetime.now() - TiempoInicio
        seconds_difference = str(time_difference.seconds)
        print(seconds_difference)
        return jsonify({
            'Estado': 'OK',
            'tiempo_de_ejecucion': '%s segundos' % (seconds_difference),
            'data':{
                'Predicciones': pred.values.tolist(),
                'Minimo': min_val,
                'Maximo': max_val
            }
        })
    except Exception as e:
        return jsonify({'Estado': 'Error', 'Data': {'Mensaje': f'Error específico: {str(e)}'}})
    finally:
        conn.close()
        print('Proceso finalizado')

@app.route('/ventas')
def prediccion_ventas():
    return {'Status': 'Running...'}

if __name__ == '__main__':
    app.run(debug=True)