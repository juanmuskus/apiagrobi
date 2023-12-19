import pandas as pd
import pmdarima as pm
from flask import Flask, request, jsonify
from sqlalchemy import URL,create_engine, text
import warnings
warnings.filterwarnings("ignore")

app = Flask(__name__)

#url_object = URL.create("mysql+mysqlconnector",username="adminbi",password="Unicordoba@23",host="ec2-18-221-11-203.us-east-2.compute.amazonaws.com",database="insumos")
url_object = URL.create("mysql+mysqlconnector",username="admin_vista",password="Unicor@2023",host="74.48.111.45",database="insumos")

class Insumo:
    def __init__(self, departamento_id, municipio_id, producto_id, cantidad_prediccion, frecuencia):
        self.departamento_id = departamento_id
        self.municipio_id = municipio_id
        self.producto_id = producto_id
        self.cantidad_prediccion = cantidad_prediccion
        self.frecuencia = frecuencia

@app.route('/')
def home():
    return {'Status': 'Running...'}

@app.route('/insumos', methods=['POST'])
def prediccion_insumo():
    data = request.get_json()
    insumo = Insumo(**data)
    df = pd.DataFrame()
    try:
        engine = create_engine(url_object)
        sql_query = text("select * from vista_precios where departamento_id = %s and municipio_id =%s and producto_id = %s" % (
            insumo.departamento_id, insumo.municipio_id, insumo.producto_id))
        df = pd.read_sql(sql_query, engine.connect())
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
        return jsonify({
            'Estado': 'OK',
            'data':{
                'Predicciones': pred.values.tolist(),
                'Minimo': min_val,
                'Maximo': max_val
            }
        })
    except Exception as e:
        print(str(e))

@app.route('/ventas')
def prediccion_ventas():
    return {'Status': 'Running...'}

if __name__ == '__main__':
    app.run(debug=True)