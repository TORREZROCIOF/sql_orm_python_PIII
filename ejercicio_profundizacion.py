#!/usr/bin/env python
'''
MELI API Introducción [Python]
Ejercicios de práctica
---------------------------
Autor: Torrez Rocio
Version: 1.0

'''

__author__ = "Torrez Rocio"
__email__ = ""
__version__ = "1.0"

import csv
import requests
import sqlalchemy
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///meli_challenge.db")
base = declarative_base()

class Producto(base):
    __tablename__ = "productos"
    id = Column(String, primary_key=True)
    site_id = Column(String)
    title = Column(String)
    price = Column(Float)
    currency_id = Column(String)
    initial_quantity = Column(Integer)
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)

    def __repr__(self):
        return f"Productp: {self.title}, Precio: {self.price}, Cantidad Disponible: {self.available_quantity}"

def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    base.metadata.drop_all(engine)
    # Crear las tablas
    base.metadata.create_all(engine)

def fill():
    print('Cargando datos desde el CSV y la API de Mercado Libre...')
    
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    with open("meli_technical_challenge_data.csv", newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            item_id = row['site'] + row['id']
            response = requests.get(f'https://api.mercadolibre.com/items?ids={item_id}') #<Response [401]> error No autorizado faltan token
            data = response.json()[0]
            
            if data["code"] == 200:
                body = data["body"]
                producto = Producto(
                    id=body["id"],
                    site_id=body["site_id"],
                    title=body["title"],
                    price=body["price"],
                    currency_id=body["currency_id"],
                    initial_quantity=body["initial_quantity"],
                    available_quantity=body["available_quantity"],
                    sold_quantity=body["sold_quantity"]
                )
                session.add(producto)
                
    session.commit()
    session.close()
    print('Datos cargados exitosamente!')

def fetch(id):
    print(f'Buscando el producto con ID: {id}...')
    
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    producto = session.query(Producto).filter_by(id=id).first()
    if producto:
        print(producto)
    else:
        print(f"No existe un producto con el ID {id} en la base de datos.")
    
    session.close()

def count_products():
    print('Contando productos...')
    
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    cantidad = session.query(Producto).count()
    print(f'Total de productos en la base de datos: {cantidad}')
    
    session.close()

if __name__ == '__main__':
    print("Bienvenidos al ejercicio de Mercado Libre")
    create_schema()  
    
    fill()          
    fetch('MLA845041373')  
    count_products()  
