import uvicorn
from database.db import DB
from fastapi import FastAPI

app = FastAPI(title='Consommation énergétique en France en 2019')

@app.get("/")
def read_root():
    return {'Hello':'World'}

@app.get("/api/nrg", tags=["infos"])
async def get_infos(
    filiere:str = None, region:str = None, recordid:str = None, commune:str = None, dptmt:str = None, operateur:str = None, complete:bool = False):
    return DB.find_infos(filiere=filiere, region=region, recordid=recordid, commune=commune, dptmt=dptmt, operateur=operateur, complete=complete)

@app.get("/api/tot", tags=["conso"])
async def get_conso(filiere:str = None, region:str = None, dptmt:str = None, commune:str = None, operateur:str = None):
    return DB.find_conso(filiere=filiere, region=region, dptmt=dptmt, commune=commune, operateur=operateur)

@app.delete("/api/rmv", tags=["handling"])
async def delete_id(recordid:str = None):
    return DB.delete_id(recordid=recordid)

@app.put("/api/upd", tags=["handling"])
async def update_doc(recordid:str, filiere:str = None, secteur:str = None, operateur:str = None, conso:float = None):
    return DB.update_item(recordid=recordid, filiere=filiere, secteur=secteur, operateur=operateur, conso=conso)

@app.get("/api/dash", include_in_schema=False)
async def get_dropdown(filiere:str = None, region:str = None, dptmt:str = None, commune:str = None, operateur:str = None):
    args = locals()
    remove = [k for k, v in args.items() if v == None]
    for k in remove: del args[k]
    return DB.find_ctgr(args)

@app.get("/api/check", include_in_schema=False)
async def check_exist(recordid:str = None):
    return {'exist': DB.check_ex(recordid)}

# if __name__ == '__main__':
#     uvicorn.run('main:app', reload=True)