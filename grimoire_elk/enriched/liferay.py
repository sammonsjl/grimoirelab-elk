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

import csv
import logging
import pkg_resources

from .enrich import Enrich, metadata

logger = logging.getLogger(__name__)


class LiferayEnrich(Enrich):

    def get_field_event_unique_id(self):
        pass

    def get_rich_events(self, item):
        pass

    def get_field_author(self):
        return "creator"

    def get_field_unique_id(self):
        return "item_id"

    def get_identities(self, item):
        """ Return the identities from an item """

        item = item['data']

        for identity in ['creator']:
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                yield user
            if 'answers' in item:
                for answer in item['answers']['items']:
                    user = self.get_sh_identity(answer[identity])
                    yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
        elif identity_field in item:
            # for answers
            user = item[identity_field]

        identity['name'] = user['userAccount']['name']
        identity['email'] = user['userAccount']['emailAddress']
        identity['username'] = user['userAccount']['alternateName']

        return identity

    @metadata
    def get_rich_item(self, item, kind='question', question_tags=None):
        eitem = {}

        # Fields common in questions and answers
        common_fields = ["headline", "numberOfMessageBoardMessages", "id",
                         "viewCount", "dateModified", "keywords", "hasValidAnswer"]

        if kind == 'question':
            self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
            # The real data
            question = item['data']

            eitem["item_id"] = question['id']
            eitem["type"] = 'question'
            eitem["author"] = None
            eitem["author"] = question['creator']['userAccount']['name']
            eitem["author_link"] = question['creator']['userAccount']['alternateName']

            # data fields to copy
            copy_fields = common_fields + ['numberOfMessageBoardMessages']
            for f in copy_fields:
                if f in question:
                    eitem[f] = question[f]
                else:
                    eitem[f] = None

            eitem["question_tags"] = question['keywords']
            eitem["question_category"] = question['taxonomyCategoryBriefs']
            # eitem["question_tags_custom_analyzed"] = question['tags']

            file = open(pkg_resources.resource_filename('grimoire_elk', 'enriched/mappings/components.csv'),
                        encoding="utf-8")
            components = csv.reader(file)

            components_dict = []

            for _ in components:
                components_dict = dict((rows[0].lower(), rows[1]) for rows in components)

            for tag in question['keywords']:
                team = components_dict.get(tag.lower())
                if team:
                    eitem["product_team"] = team
                else:
                    eitem["product_team"] = None

            if question['numberOfMessageBoardMessages'] != 0:
                eitem["is_liferay_answered"] = 1
            else:
                eitem["is_liferay_answered"] = 0

            # Fields which names are translated
            map_fields = {"headline": "question_title"}
            for fn in map_fields:
                eitem[map_fields[fn]] = question[fn]

            eitem['title_analyzed'] = question['headline']

            eitem['link'] = question['messageBoardSection']['title'].lower() + "/" + question['friendlyUrlPath']

            eitem['question_accepted_answer_id'] = None

            creation_date = question["dateCreated"]
            eitem['dateCreated'] = creation_date
            eitem.update(self.get_grimoire_fields(creation_date, "question"))

            if self.sortinghat:
                eitem.update(self.get_item_sh(item))

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

            self.add_repository_labels(eitem)
            self.add_metadata_filter_raw(eitem)

        elif kind == 'answer':
            answer = item

            eitem["type"] = 'answer'
            eitem["item_id"] = answer['id']
            eitem["author"] = None
            eitem["author"] = answer['creator']['userAccount']['name']
            eitem["author_link"] = answer['creator']['userAccount']['alternateName']

            # data fields to copy
            copy_fields = common_fields + ["origin", "tag", "dateCreated", "showAsAnswer", "id"]
            for f in copy_fields:
                if f in answer:
                    eitem[f] = answer[f]
                else:
                    eitem[f] = None

            eitem['is_accepted_answer'] = 1 if answer['showAsAnswer'] else 0
            eitem['answer_status'] = "accepted" if answer['showAsAnswer'] else "not_accepted"
            eitem['question_accepted_answer_id'] = answer['id'] if answer['showAsAnswer'] else None

            eitem["question_tags"] = question_tags
            if 'tags' in answer:
                eitem["answer_tags"] = answer['keywords']

            # Fields which names are translated
            map_fields = {"headline": "question_title"
                          }
            for fn in map_fields:
                eitem[map_fields[fn]] = answer[fn]

            eitem['link'] = answer['link']

            creation_date = answer["dateCreated"]
            eitem['creation_date'] = creation_date
            eitem.update(self.get_grimoire_fields(creation_date, "answer"))

            if self.sortinghat:
                # date field must be the same than in question to share code
                answer[self.get_field_date()] = eitem['dateCreated']
                eitem[self.get_field_date()] = eitem['dateCreated']
                eitem.update(self.get_item_sh(answer))

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

        return eitem

    def enrich_items(self, ocean_backend, events=False):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        items = ocean_backend.fetch()
        for item in items:

            if item['data']['status'] != "approved":
                continue

            answers_tags = []

            if 'answers' in item['data']:
                for answer in item['data']['answers']['items']:

                    if answer['status'] != "approved":
                        continue

                    # Copy mandatory raw fields
                    answer['origin'] = item['origin']
                    answer['tag'] = item['tag']
                    answer['link'] = item['data']['messageBoardSection']['title'].lower() + "/" + \
                                     item['data']['friendlyUrlPath']

                    rich_answer = self.get_rich_item(answer,
                                                     kind='answer',
                                                     question_tags=item['data']['keywords'])
                    if 'answer_tags' in rich_answer:
                        answers_tags.extend(rich_answer['answer_tags'])
                    items_to_enrich.append(rich_answer)

            rich_question = self.get_rich_item(item)
            rich_question['answers_tags'] = list(set(answers_tags))
            rich_question['thread_tags'] = rich_question['answers_tags'] + rich_question['question_tags']
            items_to_enrich.append(rich_question)

            if len(items_to_enrich) < self.elastic.max_items_bulk:
                continue

            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())
            items_to_enrich = []

        if len(items_to_enrich) > 0:
            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("[liferay] {}/{} missing items".format(missing, num_items))
        else:
            logger.info("[liferay] {} items inserted".format(num_items))

        return num_items
