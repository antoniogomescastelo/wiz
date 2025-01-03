import os
import json
from datetime import datetime

from models import Community, Domain, Identifier, Type, Entry, Step

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

import streamlit as st

@dataclass
class ImportService:
    run_id: Optional[str] = None
    custom_asset_import_maximum_jobs: Optional[str] = None
    custom_asset_import_maximum_entries: Optional[str] = None
    steps: Optional[dict] = field(default_factory=dict) 
    def get_domain(self, domain_community, domain_type, domain_name):
        community = Community()
        community.name=domain_community

        type_ = Type()
        type_.name = domain_type

        identifier = Identifier()
        identifier.name = domain_name
        identifier.community = community

        entry = Entry()
        entry.resourceType = "Domain"
        entry.identifier = identifier
        entry.type = type_

        return entry

    def get_asset(self, asset_community, asset_domain, asset_type, asset_name, asset_display_name):
        community = Community()
        community.name = asset_community

        domain = Domain()
        domain.name = asset_domain
        domain.community = community

        type_ = Type()
        type_.name = asset_type

        identifier = Identifier()
        identifier.name = asset_name
        identifier.domain = domain

        entry = Entry()
        entry.resourceType = "Asset"
        entry.identifier = identifier
        entry.type = type_
        entry.displayName = asset_display_name

        return entry

    def add_attributes(self, entry, name, value, type):
        if name not in entry.attributes:
            entry.attributes[name] = []

        if type == "date":
            entry.attributes[name].append(self.get_date_as_string(int(value)))

        if type == "array":
            entry.attributes[name].append(json.dumps(value, indent=4))

        if type == "string":
            entry.attributes[name].append(value)

    def add_relations(self, entry, relation_type, relation_target, asset_domain, asset_community, asset_name):
        name = f"{relation_type}:{relation_target}"

        community = Community()
        community.name = asset_community

        domain = Domain()
        domain.name = asset_domain
        domain.community = community

        identifier = Identifier()
        identifier.name = asset_name
        identifier.domain = domain
        
        if name not in entry.relations:
            entry.relations[name] = []

        entry.relations[name].append(identifier)

    def get_date_as_string(self, unix_timestamp):
        return datetime.fromtimestamp(unix_timestamp).strftime("%a, %d %b %Y %H:%M:%S %Z")

    def save(self, entries, resource_location, file_name, step_number, split):
        number_of_files = 1

        if split:
            number_of_files = max(int(-(-len(entries) // self.custom_asset_import_maximum_entries)), self.custom_asset_import_maximum_jobs)

        number_of_entries_per_file = -(-len(entries) // number_of_files)

        from_index = 0

        for i in range(number_of_files):
            path = os.path.join(resource_location, self.run_id)

            try:
                os.makedirs(path, exist_ok=True)

            except Exception:
                pass

            x = entries[from_index: min(from_index + number_of_entries_per_file, len(entries))]

            name = f"{resource_location}/{self.run_id}/{step_number}.{file_name}.{i}.json"

            with open(name, 'w') as file:
                json.dump(x, file, default=lambda o: {k: v for k, v in o.__dict__.items() if v})

            step = Step(step_number, path, file_name, i)

            if step.step_number not in self.steps:
                self.steps[step.step_number] = []

            self.steps[step.step_number].append(step)

            from_index += number_of_entries_per_file

            if from_index >= len(entries):
                break

        name = f"{resource_location}/{self.run_id}.json"

        with open(name, 'w') as file:
            json.dump(self, file, default=lambda o: o.__dict__)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, skipkeys=True)

    def __str__(self):
        return f"ImportService [customAssetImportMaximumJobs={self.custom_asset_import_maximum_jobs}, customAssetImportMaximumEntries={self.custom_asset_import_maximum_entries}, runId={self.run_id}, steps={self.steps}]"
    