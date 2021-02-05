import pymongo

try:
    from database.ids import user, passw, db
except:
    user = 'stephane_isen'
    passw = 'isenBrest_29'
    db = 'brief_api'

fields = {'region':'fields.libelle_region', 'filiere':'fields.filiere', 'commune':'fields.libelle_commune',
        'dptmt':'fields.libelle_departement', 'operateur':'fields.operateur', 'recordid':'recordid', 
        'secteur':'fields.libelle_grand_secteur', 'conso':'fields.conso'}

# Classe de la connection à la base de données
class DB():

    # Méthode pour se connecter à la bdd
    @classmethod
    def login(cls, user=user, passw=passw, db=None):
        return f"mongodb+srv://{user}:{passw}@clusterkata.b6v13.mongodb.net/{db}?retryWrites=true&w=majority"

    # Méthode pour ouvrir la connection à la bdd
    @classmethod
    def open_con(cls):
        cls.client = pymongo.MongoClient(cls.login())
        cls.db = cls.client.get_database(db)
        cls.collection = cls.db.energie

    # Méthode pour fermer la connection à la bdd
    @classmethod
    def close_con(cls):
        cls.client.close()

    # Méthode pour définir les query du find
    @classmethod
    def set_match(cls, arguments):
        match = {}
        for k, v in arguments.items():
            if k != 'recordid':
                if v != None and v != False and v != True and v != 'None':
                    match[fields[k]] = v
        return match

    # Méthode pour définir les query du distinct
    @classmethod
    def set_match_distinct(cls, arguments):
        for k, v in arguments.items():
            if v == 'all':
                return fields[k]

    # Méthode pour définir les query filtres du find
    @classmethod
    def set_filter(cls, arguments):
        filters = {}
        for k, v in arguments.items():
            if k == 'complete' and v == False:
                filters = {'_id':0, 'recordid':1, 'fields.filiere':1, 'fields.libelle_commune':1, 'fields.conso':1, 'fields.operateur':1}
            elif k == 'complete' and v == True:
                filters = {'_id':0}
        return filters

    # Méthode pour définir les query filtres pour une seule catégorie
    @classmethod
    def set_filter_distinct(cls, arguments):
        filters = {}
        for k, v in arguments.items():
            if v != None and v != 'all' and v != 'None':
                filters[fields[k]] = v
        return filters

    # Méthode pour update kwargs pour la conso
    @classmethod
    def set_kwargs(cls, arguments, data):
        copy = arguments.copy()
        for k, v in arguments.items():
            if v == None or v == 'None':
                copy.pop(k)
        copy.update({'conso_tot':list(data)[0]['count']})
        return copy

    # Méthode pour récupérer les infos de consommation
    @classmethod
    def find_infos(cls, **kwargs):
        cls.open_con()
        data = list(cls.collection.find(cls.set_match(kwargs), cls.set_filter(kwargs)))
        cls.close_con()
        return {'data': data}

    # Méthode pour récupérer les totaux de consommation
    @classmethod
    def find_conso(cls, **kwargs):
        cls.open_con()
        try:
            data = list(cls.collection.aggregate([{'$match':cls.set_match(kwargs)}, {'$group':{'_id':'conso_tot','count':{'$sum':'$fields.conso'}}}]))
            ret = cls.set_kwargs(kwargs, data)
        except IndexError:
            ret = 'incorrect query, control your arguments writing'
        cls.close_con()
        return {'data': ret}

    # Méthode pour supprimer un enregistrement
    @classmethod
    def delete_id(cls, recordid):
        cls.open_con()
        cls.collection.delete_one({'recordid': recordid})
        cls.close_con()
        return {'delete': f'success deleting {recordid}'}

    # Méthode pour mettre à jour un enregistrement
    @classmethod
    def update_item(cls, **kwargs):
        cls.open_con()
        data = list(cls.collection.update({'recordid':kwargs['recordid']}, {'$set': cls.set_match(kwargs)}))
        cls.close_con()
        return {'update':''}

    # Méthode pour récupérer les menus déroulants
    @classmethod
    def find_ctgr(cls, arguments):
        cls.open_con()
        data = list(cls.collection.distinct(cls.set_match_distinct(arguments), cls.set_filter_distinct(arguments)))
        cls.close_con()
        return {'data': data}

    # Méthode pour check l'existence d'un item
    @classmethod
    def check_ex(cls, record):
        cls.open_con()
        exist = False
        request = list(cls.collection.find({'recordid':record}, {'_id':0, 'recordid':1}))
        if len(request)  > 0:
            exist = True
        cls.close_con()
        return exist