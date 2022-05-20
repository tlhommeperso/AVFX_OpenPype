import time
from avalon.api import AvalonMongoDB


class LoaderSelectionContext:
    def __init__(self):
        self._project_name = None
        self._asset_ids = []
        self._version_ids = []

    @property
    def project_name(self):
        return self._project_name

    @property
    def asset_ids(self):
        return self._asset_ids

    @property
    def version_ids(self):
        return self._version_ids

    def set_project_name(self, project_name):
        self._project_name = project_name
        self.set_asset_ids([])

    def set_asset_ids(self, asset_ids):
        self._asset_ids = asset_ids
        self.set_version_ids([])

    def set_version_ids(self, version_ids):
        self._version_ids = version_ids


class LoadController:
    def __init__(self):
        dbcon = AvalonMongoDB()

        self._context = LoaderSelectionContext()
        self._model = LoaderModel(dbcon)
        self._dbcon = dbcon

    def set_project(self, project_name):
        self._context.set_project_name(project_name)

    def set_asset_ids(self, asset_ids):
        self._context.set_asset_ids(asset_ids)



class DataCache:
    def __init__(self, data, timeout=None):
        if timeout is None:
            timeout = 10
        self._timeout = timeout
        self._data = data
        self._end_time = time.time() + timeout

    @property
    def is_outdated(self):
        return time.time() > self._end_time

    @property
    def data(self):
        return self._data


class LoaderModel:
    asset_doc_projection = {
        "_id": 1,
        "name": 1,
        "label": 1
    }
    subset_doc_projection = {
        "name": 1,
        "parent": 1,
        "schema": 1,
        "data.families": 1,
        "data.subsetGroup": 1
    }

    def __init__(self, dbcon):
        self._dbcon = dbcon
        self._asset_docs_cache = {}
        self._subset_docs_cache = {}
        self._families_cache = {}

    def set_project(self, project_name):
        if project_name == self._loaded_project:
            return
        self._loaded_project = project_name

    def get_assets(self, project_name):
        if not project_name:
            return []

        asset_cache = self._asset_docs_cache.get(project_name)
        if asset_cache is not None:
            if not asset_cache.is_outdated:
                return asset_cache.data
        self._cache_asset_docs(project_name)

        return self._asset_docs_cache[project_name].data

    def get_subset_families(self, project_name):
        if not project_name:
            return set()

        cache = self._families_cache.get(project_name)
        if cache is not None:
            if not cache.is_outdated:
                return cache.data
        self._cache_families(project_name)
        return self._families_cache[project_name].data

    def get_subsets_by_asset_ids(self, asset_ids, project_name):
        if not asset_ids:
            return {}

        if project_name not in self._subset_docs_cache:
            self._subset_docs_cache[project_name] = {}

        project_cache = self._subset_docs_cache[project_name]
        asset_ids_set = set(asset_ids)
        for asset_id in tuple(project_cache.keys()):
            cache = project_cache[asset_id]
            if cache.is_outdated:
                project_cache.pop(asset_id)
            elif asset_id in asset_ids_set:
                asset_ids_set.remove(asset_id)

        if asset_ids_set:
            self._dbcon.install()
            database = self._dbcon.database
            subset_docs = database[project_name].find(
                {
                    "type": "subset",
                    "parent": {"$in": list(asset_ids_set)}
                },
                self.subset_doc_projection
            )
            _subsets_by_asset_id = {}
            for subset_doc in subset_docs:
                asset_id = subset_doc["parent"]
                if asset_id not in _subsets_by_asset_id:
                    _subsets_by_asset_id[asset_id] = []
                _subsets_by_asset_id[asset_id].append(subset_doc)

            for key, value in _subsets_by_asset_id.items():
                project_cache[key] = DataCache(value)

        subset_docs_by_id = {}
        for asset_id in asset_ids:
            subset_docs_by_id[asset_id] = project_cache[asset_id].data
        return subset_docs_by_id

    def _cache_asset_docs(self, project_name):
        self._dbcon.install()
        database = self._dbcon.database
        asset_docs = list(database[project_name].find({
            "type": "asset"
        }))
        self._asset_docs_cache[project_name] = DataCache(asset_docs)

    def _cache_families(self, project_name):
        self._dbcon.install()
        database = self._dbcon.database
        families = set()
        result = list(database[project_name].aggregate([
            {"$match": {
                "type": "subset"
            }},
            {"$project": {
                "family": {"$arrayElemAt": ["$data.families", 0]}
            }},
            {"$group": {
                "_id": "family_group",
                "families": {"$addToSet": "$family"}
            }}
        ]))
        if result:
            families = set(result[0]["families"])

        self._families_cache[project_name] = DataCache(families)

    def clear_cache(self, project_name=None):
        cols = (
            self._asset_docs_cache,
            self._families_cache
        )
        if project_name is None:
            for col in cols:
                for project_name in tuple(col.keys()):
                    col.pop(project_name)
        else:
            for col in cols:
                if project_name in col:
                    col.pop(project_name)
