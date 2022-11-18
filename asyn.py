import asyncio
import aiohttp
import requests
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

d_base = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/star_wars'
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
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


def get_count():
    return requests.get('https://swapi.dev/api/people/').json()['count'] + 1


async def get_stuff(number):
    session = aiohttp.ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/{number}')
    if response.status == 200:
        json_data = await response.json()
        await session.close()
        return json_data
    else:
        await session.close()
        return 'err'


async def main():
    async with engine.begin() as connect:
        await connect.run_sync(Base.metadata.create_all)
        await connect.commit()
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    number = 1
    for item in range(0, get_count()):
        res = await get_stuff(number)
        if res != 'err':
            fil = ', '.join(res['films'])
            spe = ', '.join(res['species'])
            sta = ', '.join(res['starships'])
            veh = ', '.join(res['vehicles'])

            number += 1
            async with Session() as session:
                session.add(StarWars(
                    name=str(res['name']),
                    birth_year=str(res['birth_year']),
                    eye_color=str(res['eye_color']),
                    films=str(fil),
                    gender=str(res['gender']),
                    hair_color=str(res['hair_color']),
                    height=str(res['height']),
                    homeworld=str(res['homeworld']),
                    mass=str(res['mass']),
                    skin_color=str(res['birth_year']),
                    species=str(spe),
                    starships=str(sta),
                    vehicles=str(veh)
                ))
                await session.commit()
                await session.close()

        else:
            number += 1
            print('404 not found')

asyncio.run(main())
