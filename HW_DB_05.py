# Домашнее задание к лекции «Python и БД. ORM»

import json
import os
import sqlalchemy

import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

# CREATE TABLE
class Publisher(Base):
    __tablename__ = "Publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)

    def __str__(self):  # для удобства извлчения данных
        return f'Publisher: {self.id}: {self.name}'
    
    
class Book(Base):
    __tablename__ = "Book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=140), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("Publisher.id"), nullable=False)
            
    publisher = relationship(Publisher, backref="Books")
        
    def __str__(self):  # для удобства извлчения данных
        return f'Book: {self.id}: ({self.title}, {self.id_publisher})'
    
    
class Shop(Base):
    __tablename__ = "Shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=140), nullable=False)

    def __str__(self):  # для удобства извлчения данных
        return f'Shop: {self.id}: {self.name}'


class Stock(Base):
    __tablename__ = "Stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("Shop.id"), nullable=False)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("Book.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship(Book, backref="Books")
    shop = relationship(Shop, backref="Shops")

    def __str__(self):  # для удобства извлчения данных
        return f'Stock: {self.id}: ({self.id_book}, {self.id_shop}, {self.count})'
    
    
class Sale(Base):
    __tablename__ = "Sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.String(length=20), nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("Stock.id"), nullable=False)
    
    stock = relationship(Stock, backref="Sales")

    def __str__(self):  # для удобства извлчения данных
        return f'Sale: {self.id}: ({self.price}, {self.date_sale}, {self.count}, {self.id_stock})'


def create_tables(engine):
    Base.metadata.drop_all(engine)  # Удаление всех таблиц из БД
    Base.metadata.create_all(engine)
    print('База создана')


# Параметры подключения    
s_BD = 'postgresql'
s_BD_lgn = 'postgres'
s_BD_psswrd = 'postgres'
s_BD_lclhst = '5432'
s_BD_name = 'HW_DB_05'
    
DSN = f'{s_BD}://{s_BD_lgn}:{s_BD_psswrd}@localhost:{s_BD_lclhst}/{s_BD_name}'
engine = sqlalchemy.create_engine(DSN) # create_engine() - движок для подключения к базе данных, принимает на вход DSN URL
create_tables(engine) # Вызываем функцию создания таблиц

Session = sessionmaker(bind=engine) # sessionmaker - объект принимает на вход движок и создаёт сессию
session = Session()


# Заполнение таблицы с помощью создания моделей
file_path = os.path.join(os.getcwd(), 'HW_All/DB/HW_DB_05/tests_data.json')  ## строительство путик нашему файлу
with open(file_path, 'r') as fd:
    data = json.load(fd)

for element in data:
    if element.get('model') == 'publisher':
        obj = Publisher\
                (id=element.get('pk'),\
                name=element.get('fields', {}).get('name', {}))
        session.add(obj)
    if element.get('model') == 'book':
        obj = Book\
                (id=element.get('pk'),\
                title=element.get('fields', {}).get('title', {}),\
                id_publisher=element.get('fields', {}).get('id_publisher', {}))
        session.add(obj)
    if element.get('model') == 'shop':
        obj = Shop\
                (id=element.get('pk'),\
                name=element.get('fields', {}).get('name', {}))
        session.add(obj)
    if element.get('model') == 'stock':
        obj = Stock\
                (id=element.get('pk'),\
                id_shop=element.get('fields', {}).get('id_shop', {}),\
                id_book=element.get('fields', {}).get('id_book', {}),\
                count=element.get('fields', {}).get('count', {}))
        session.add(obj)
    if element.get('model') == 'sale':
        obj = Sale\
                (id=element.get('pk'),\
                price=element.get('fields', {}).get('price', {}),\
                date_sale=element.get('fields', {}).get('date_sale', {}),\
                count=element.get('fields', {}).get('count', {}),\
                id_stock=element.get('fields', {}).get('id_stock', {}))
        session.add(obj)

# (Более изящное решение в подсказке)   
# for element in data:
#     print(element)
#     model = {
#         'publisher': Publisher,
#         'shop': Shop,
#         'book': Book,
#         'stock': Stock,
#         'sale': Sale,
#     }[element.get('model')]
#     print(model, element.get('model'))
#     #print(element.get('model'))
#     session.add(model(id=element.get('pk'), **element.get('fields')))

session.commit()

# Проба через подзапросы, неудачно
# def Z_P(name = input('Введите имя издателя\n') ):
#     subq_01 = session.query(Book).join(Publisher.Books).filter(Publisher.name.like(name)).subquery('sub_01')
#     subq_02 = session.query(Stock).join(subq_01, Stock.id_book == subq_01.c.id).subquery("sub_02")
#     subq_03 = session.query(Shop).join(subq_02,Shop.id == subq_02.c.id_shop).subquery("sub_03")
#     subq_04 = session.query(Sale).join(subq_03,Sale.id_stock == subq_03.c.id).all()
#     #print(type(subq))
#     # for i in subq:
#     #     print(i.title)

#     subq_01 = session.query(Book).join(Publisher.Books).filter(Publisher.name.like(name))
#     for b in subq_01:
#         print(b.title)
    
#     subq_03 = session.query(Shop).join(subq_02,Shop.id == subq_02.c.id_shop).all()
#     for shop in subq_03:
#         print(shop.name)
        
#     #subq_02 = session.query(Stock).join(subq_01, Stock.id_book == subq_01.c.id)
#     for stock in subq_02:
#         print(stock.id)
    
#     #subq_04 = session.query(Sale).join(subq_03,Sale.id_stock == subq_03.c.id).all()
    
#     for a in subq_04:
#         print(a.price, a.date_sale)

def Z_P(pub_name = input('Введите имя издателя для просмотра продаж\n') ):
    print()
    subq_03 = session.query(Sale).join(Stock.Sales).join(Book).join(Publisher).filter(Publisher.name.like(pub_name))
    for b in subq_03.all():
        #print(b.stock.Sales)  # Такой способ вернуться по ссылкам в нужные таблицы
        #print(b.stock.book)
        #print(b.stock.shop)
        #print(b.price, b.date_sale)
        #print(b.stock.book.title)
        
        print('{0:40} | {1:10} | {2:5} | {3:}'.format(b.stock.book.title, b.stock.shop.name, b.price, b.date_sale))
    
Z_P()