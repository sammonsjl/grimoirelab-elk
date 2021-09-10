# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import logging

from .enrich import Enrich, metadata

logger = logging.getLogger(__name__)


class GitHubReleasesEnrich(Enrich):

    def get_field_event_unique_id(self):
        pass

    def get_rich_events(self, item):
        pass

    def get_field_author(self):
        return "login"

    def get_identities(self, item):
        """ Return the identities from an item """
        # In DockerHub there are no identities. Just the organization and
        # the repository name for the docker image
        identities = []
        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        # The real data
        release = item['data']

        # data fields to copy
        copy_fields = ["name", "assets"]
        for f in copy_fields:
            if f in release:
                eitem[f] = release[f]
            else:
                eitem[f] = None

        # Fields which names are translated
        map_fields = {}
        for fn in map_fields:
            eitem[map_fields[fn]] = release[fn]

        eitem["id"] = release["id"]

        eitem['created_at'] = release['created_at']

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "github"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem
