import asyncio
import aiohttp
import requests
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

d_base = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:1111/star_wars'
engine = create_async_engine(d_base)

Base = declarative_base()


class StarWars(Base):

    __tablename__ = 'SW'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(Integer)
    homeworld = Column(String)
    mass = Column(Integer)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


def get_count():
    return requests.get('https://swapi.dev/api/people/').json()['count']


async def get_stuff(number):
    session = aiohttp.ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/{number}')
    json_data = await response.json()
    await session.close()
    return json_data


async def main():
    async with engine.begin() as connect:
        await connect.run_sync(Base.metadata.create_all)
        await connect.commit()
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    number = 1
    for item in range(0, get_count()):
        res = await get_stuff(number)
        number += 1
        async with Session as session:
            session.add()
            await session.commit()

asyncio.run(main())