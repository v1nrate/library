from pickle import TRUE
from sqlalchemy.orm import DeclarativeBase, sessionmaker, relationship
from sqlalchemy import create_engine, Column, ForeignKey, String, Integer
import csv
import json
import xml.etree.ElementTree as ET

class Base(DeclarativeBase):
    pass

pre = 'rus_'

class AuthorModel(Base):
    __tablename__ = pre + 'authors'
    
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)
    
    books = relationship('BookModel', back_populates='author')

class PublisherModel(Base):
    __tablename__ = pre + 'publishers'
    
    id = Column(Integer, autoincrement=True, primary_key=True)
    title = Column(String, nullable=False, unique=True)
    
    books = relationship('BookModel', back_populates='publisher')  # Исправлено с publisher на books

class BookModel(Base):
    __tablename__ = pre + 'books'
    
    id = Column(Integer, autoincrement=True, primary_key=True)
    title = Column(String, nullable=False)
    year = Column(Integer)
    author_id = Column(Integer, ForeignKey(AuthorModel.id, ondelete='CASCADE'))  # Исправлено Integrr на Integer
    publisher_id = Column(Integer, ForeignKey(PublisherModel.id, ondelete='CASCADE'))
    
    author = relationship('AuthorModel', back_populates='books')
    publisher = relationship('PublisherModel', back_populates='books')

# Данные подключения к Supabase
USER = 'postgres.cxncgolzxhrrcykcejoe'
PASS = 's%r6tuiGaWA3jW'
HOST = 'aws-0-eu-north-1.pooler.supabase.com'
PORT = 6543
NAME = 'postgres'

DB_URL = f"postgresql+psycopg2://{USER}:{PASS}@{HOST}:{PORT}/{NAME}?sslmode=require"

engine = create_engine(DB_URL)
# Base.metadata.create_all(engine)

Session = sessionmaker(engine)
session = Session()


class AuthorService():
    def __init__(self):
        self._cache={}
            
    def create(self, name):
        author = self._cache.get(name)
        if author:
            return author
        author = AuthorModel(name=name)
        session.add(author)
        session.commit()
        self._cache[author.name] = author
        return author
    
class PublisherService():
    def __init__(self):
        self._cache={}
    
    def create(self, name):
        publisher = self._cache.get(name)
        if publisher:
            return publisher
        publisher = PublisherModel(title=name)
        session.add(publisher)
        session.commit()
        self._cache[publisher.title] = publisher
        return publisher
    
class BookService():
    def create(self, title, year, author_id, publisher_id):
        book = BookModel(
            title = title, 
            year = year, 
            author_id = author_id, 
            publisher_id = publisher_id
        )
        session.add(book)
        session.commit()
        return book

    def get_by_title(self, title):
        book = session.query(BookModel).filter_by(title=title).first()
        return book
    
    def get_by_publisher(self, title):
        publisher = session.query(PublisherModel).filter_by(title=title).first()
        if publisher:
            return publisher.books
        return []
        

auth_s = AuthorService()
publ_s = PublisherService()
book_s = BookService()

book = book_s.get_by_title('Идиот')

# if book:
#     print(book.id) 
#     print(book.title)   
#     print(book.year)    
#     print(book.publisher.title)  
#     print(book.author.name)
# else:
#     print('Not found')    

books = book_s.get_by_publisher('Азбука')

book_list = [{
    'id': book.id,
    'title': book.title,
    'year': book.year,
    'author': book.author.name,
    'publisher': book.publisher.title
} for book in books]
# print(book_list)
with open('azbook.json', 'w', encoding='UTF-8') as f:
    json.dump(book_list, f, indent=4)

root= ET.Element('')
for b in books:
    book = ET.SubElement(root, 'book')
    ET.SubElement(book, 'title').text = b.title
    ET.SubElement(book, 'year').text = str(b.year)

ET.indent(root, space="    ") 
    
tree = ET.ElementTree(root)
tree.write('new_books.xml', encoding='utf-8', xml_declaration=True)

# for book in books:
#     print(book.id) 
#     print(book.title)   
#     print(book.year)    
#     print(book.publisher.title)  
#     print(book.author.name)












# Чтение CSV файла
# with open('books.csv', 'r', encoding='utf-8') as f:
#     csv_reader = csv.DictReader(f)
#     for row in csv_reader:
#         author = auth_s.create(row['Автор'])
#         publisher = publ_s.create(row['Издательство'])
#         book = book_s.create(
#             row['Название книги'],
#             row['Год выпуска'],
#             author.id,
#             publisher.id
#         )
        