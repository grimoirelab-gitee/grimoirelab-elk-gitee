# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
#   Haiming Lin <lhming23@outlook.com>
#

import logging
import re
import time

import requests

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import pandas as pd

from grimoire_elk.elastic import ElasticSearch
from grimoire_elk.errors import ELKError
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection

from grimoire_elk.enriched.utils import get_time_diff_days

from grimoire_elk.enriched.enrich import Enrich, metadata
from grimoire_elk.elastic_mapping import Mapping as BaseMapping
from grimoire_elk.enriched.graal_study_evolution import (get_to_date,
                                                         get_unique_repository)

from perceval.backend import uuid


GITEE = 'https://gitee.com/'
GITEE_ISSUES = "gitee_issues"
GITEE_MERGES = "gitee_pulls"

logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.
        geopoints type is not created in dynamic mapping
        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "merge_author_geolocation": {
                   "type": "geo_point"
               },
               "assignee_geolocation": {
                   "type": "geo_point"
               },
               "state": {
                   "type": "keyword"
               },
               "user_geolocation": {
                   "type": "geo_point"
               },
               "title_analyzed": {
                 "type": "text",
                 "index": true
               }
            }
        }
        """

        return {"items": mapping}


class GiteeEnrich(Enrich):

    mapping = Mapping

    issue_roles = ['assignee_data', 'user_data']
    pr_roles = ['merged_by_data', 'user_data']
    roles = ['assignee_data', 'merged_by_data', 'user_data']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_onion)
        self.studies.append(self.enrich_ossf_activity)
        # self.studies.append(self.enrich_pull_requests)
        # self.studies.append(self.enrich_geolocation)
        # self.studies.append(self.enrich_extra_data)
        # self.studies.append(self.enrich_backlog_analysis)

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "user_data"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_identities(self, item):
        """Return the identities from an item"""

        category = item['category']
        item = item['data']

        if category == "issue":
            identity_types = ['user', 'assignee']
        elif category == "pull_request":
            identity_types = ['user', 'merged_by']
        else:
            identity_types = []

        for identity in identity_types:
            identity_attr = identity + "_data"
            if item[identity] and identity_attr in item:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity_attr])
                if user:
                    yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['login']
        identity['email'] = None
        identity['name'] = None
        if 'email' in user:
            identity['email'] = user['email']
        if 'name' in user:
            identity['name'] = user['name']
        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    def get_time_to_first_attention(self, item):
        """Get the first date at which a comment was made to the issue by someone
        other than the user who created the issue
        """
        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']
                         if item['user']['login'] != comment['user']['login']]
        if comment_dates:
            return min(comment_dates)
        return None
    #get comments and exclude bot

    def get_num_of_comments_without_bot(self, item):
        """Get the num of comment was made to the issue by someone
        other than the user who created the issue and bot
        """
        comments = [comment for comment in item['comments_data']
                    if item['user']['login'] != comment['user']['login'] \
                    and not (comment['user']['name'].endswith("bot"))]
        return len(comments)

    #get first attendtion without bot
    def get_time_to_first_attention_without_bot(self, item):
        """Get the first date at which a comment was made to the issue by someone
        other than the user who created the issue and bot
        """
        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']
                         if item['user']['login'] != comment['user']['login'] \
                         and not (comment['user']['name'].endswith("bot"))]
        if comment_dates:
            return min(comment_dates)
        return None

    def get_time_to_merge_request_response(self, item):
        """Get the first date at which a review was made on the PR by someone
        other than the user who created the PR
        """
        review_dates = []
        for comment in item['review_comments_data']:
            # skip comments of ghost users
            if not comment['user']:
                continue

            # skip comments of the pull request creator
            if item['user']['login'] == comment['user']['login']:
                continue

            review_dates.append(str_to_datetime(comment['created_at']))

        if review_dates:
            return min(review_dates)

        return None

    def get_latest_comment_date(self, item):
        """Get the date of the latest comment on the issue/pr"""

        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']]
        if comment_dates:
            return max(comment_dates)
        return None

    def get_num_commenters(self, item):
        """Get the number of unique people who commented on the issue/pr"""

        commenters = [comment['user']['login'] for comment in item['comments_data']]
        return len(set(commenters))

    def get_CVE_message(self, item):
        """Get the first date at which a comment was made to the issue by someone
        other than the user who created the issue and bot
        """
        if item["body"] and "漏洞公开时间" in item["body"]:
            issue_body = item["body"].splitlines()
            cve_body = {}
            for message in issue_body:
                try:
                    [key, val] = message.split('：')
                    cve_body[key.strip()] = val.strip()
                except Exception as e:
                    pass
            return cve_body
        else:
            return None

    @metadata
    def get_rich_item(self, item):

        rich_item = {}
        if item['category'] == 'issue':
            rich_item = self.__get_rich_issue(item)
        elif item['category'] == 'pull_request':
            rich_item = self.__get_rich_pull(item)
        elif item['category'] == 'repository':
            rich_item = self.__get_rich_repo(item)
        else:
            logger.error("[github] rich item not defined for GitHub category {}".format(
                         item['category']))

        self.add_repository_labels(rich_item)
        self.add_metadata_filter_raw(rich_item)
        return rich_item

    def __get_rich_pull(self, item):
        rich_pr = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_pr[f] = item[f]
            else:
                rich_pr[f] = None
        # The real data
        pull_request = item['data']

        #close and merge in gitee are two different status
        if pull_request['state'] == 'merged':
            rich_pr['time_to_close_days'] = \
                get_time_diff_days(pull_request['created_at'], pull_request['merged_at'])
        else:
            rich_pr['time_to_close_days'] = \
                get_time_diff_days(pull_request['created_at'], pull_request['closed_at'])

        #merged is not equal to closed in gitee
        if pull_request['state'] == 'open':
            rich_pr['time_open_days'] = \
                get_time_diff_days(pull_request['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_pr['time_open_days'] = rich_pr['time_to_close_days']

        rich_pr['user_login'] = pull_request['user']['login']

        user = pull_request.get('user_data', None)
        if user is not None and user:
            rich_pr['user_name'] = user['name']
            rich_pr['author_name'] = user['name']
            rich_pr["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_pr['user_org'] = user.get('company', None)
            rich_pr['user_location'] = user.get('location', None)
            rich_pr['user_geolocation'] = None
        else:
            rich_pr['user_name'] = None
            rich_pr["user_domain"] = None
            rich_pr['user_org'] = None
            rich_pr['user_location'] = None
            rich_pr['user_geolocation'] = None
            rich_pr['author_name'] = None

        merged_by = pull_request.get('merged_by_data', None)
        if merged_by and merged_by is not None:
            rich_pr['merge_author_login'] = merged_by['login']
            rich_pr['merge_author_name'] = merged_by['name']
            rich_pr["merge_author_domain"] = self.get_email_domain(merged_by['email']) if merged_by.get('email', None) else None
            rich_pr['merge_author_org'] = merged_by.get('company', None)
            rich_pr['merge_author_location'] = merged_by.get('location', None)
            rich_pr['merge_author_geolocation'] = None
        else:
            rich_pr['merge_author_name'] = None
            rich_pr['merge_author_login'] = None
            rich_pr["merge_author_domain"] = None
            rich_pr['merge_author_org'] = None
            rich_pr['merge_author_location'] = None
            rich_pr['merge_author_geolocation'] = None

        rich_pr['id'] = pull_request['id']
        rich_pr['id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['repository'] = self.get_project_repository(rich_pr)
        rich_pr['title'] = pull_request['title']
        rich_pr['title_analyzed'] = pull_request['title']
        rich_pr['state'] = pull_request['state']
        rich_pr['created_at'] = pull_request['created_at']
        rich_pr['updated_at'] = pull_request['updated_at']
        rich_pr['merged'] = pull_request['state'] == 'merged'
        rich_pr['merged_at'] = pull_request['merged_at']
        rich_pr['closed_at'] = pull_request['closed_at']
        rich_pr['url'] = pull_request['html_url']
        labels = []
        [labels.append(label['name']) for label in pull_request['labels'] if 'labels' in pull_request]
        rich_pr['labels'] = labels

        rich_pr['pull_request'] = True
        rich_pr['item_type'] = 'pull request'

        rich_pr['gitee_repo'] = rich_pr['repository'].replace(GITEE, '')
        rich_pr['gitee_repo'] = re.sub('.git$', '', rich_pr['gitee_repo'])
        rich_pr["url_id"] = rich_pr['gitee_repo'] + "/pull/" + rich_pr['id_in_repo']

        # GMD code development metrics
        rich_pr['forks'] = None
        rich_pr['code_merge_duration'] = get_time_diff_days(pull_request['created_at'],
                                                            pull_request['merged_at'])
        rich_pr['num_review_comments'] = len(pull_request['review_comments_data'])

        rich_pr['time_to_merge_request_response'] = None
        if rich_pr['num_review_comments'] != 0:
            min_review_date = self.get_time_to_merge_request_response(pull_request)
            rich_pr['time_to_merge_request_response'] = \
                get_time_diff_days(str_to_datetime(pull_request['created_at']), min_review_date)

        if self.prjs_map:
            rich_pr.update(self.get_item_project(rich_pr))

        if 'project' in item:
            rich_pr['project'] = item['project']

        rich_pr.update(self.get_grimoire_fields(pull_request['created_at'], "pull_request"))

        item[self.get_field_date()] = rich_pr[self.get_field_date()]
        rich_pr.update(self.get_item_sh(item, self.pr_roles))

        return rich_pr

    def __get_rich_issue(self, item):
        rich_issue = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_issue[f] = item[f]
            else:
                rich_issue[f] = None
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['finished_at'])

        #issue have four status: open,progressing, closed, rejected.
        if issue['state'] == 'open' or issue['state'] == 'progressing':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['user_login'] = issue['user']['login']

        user = issue.get('user_data', None)
        if user is not None and user:
            rich_issue['user_name'] = user['name']
            rich_issue['author_name'] = user['name']
            rich_issue["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_issue['user_org'] = user.get('company', None)
            rich_issue['user_location'] = user.get('location', None)
            rich_issue['user_geolocation'] = None
        else:
            rich_issue['user_name'] = None
            rich_issue["user_domain"] = None
            rich_issue['user_org'] = None
            rich_issue['user_location'] = None
            rich_issue['user_geolocation'] = None
            rich_issue['author_name'] = None

        assignee = issue.get('assignee_data', None)
        if assignee and assignee is not None:
            assignee = issue['assignee_data']
            rich_issue['assignee_login'] = assignee['login']
            rich_issue['assignee_name'] = assignee['name']
            rich_issue["assignee_domain"] = self.get_email_domain(assignee['email']) if assignee.get('email', None) else None
            rich_issue['assignee_org'] = assignee.get('company', None)
            rich_issue['assignee_location'] = assignee.get('location', None)
            rich_issue['assignee_geolocation'] = None
        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            rich_issue["assignee_domain"] = None
            rich_issue['assignee_org'] = None
            rich_issue['assignee_location'] = None
            rich_issue['assignee_geolocation'] = None

        rich_issue['id'] = issue['id']
        rich_issue['id_in_repo'] = issue['html_url'].split("/")[-1]
        rich_issue['repository'] = self.get_project_repository(rich_issue)
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['finished_at']
        rich_issue['url'] = issue['html_url']
        rich_issue['issue_type'] = issue['issue_type']
        labels = []
        [labels.append(label['name']) for label in issue['labels'] if 'labels' in issue]
        rich_issue['labels'] = labels

        rich_issue['pull_request'] = True
        rich_issue['item_type'] = 'pull request'
        if 'head' not in issue.keys() and 'pull_request' not in issue.keys():
            rich_issue['pull_request'] = False
            rich_issue['item_type'] = 'issue'

        rich_issue['gitee_repo'] = rich_issue['repository'].replace(GITEE, '')
        rich_issue['gitee_repo'] = re.sub('.git$', '', rich_issue['gitee_repo'])
        rich_issue["url_id"] = rich_issue['gitee_repo'] + "/issues/" + rich_issue['id_in_repo']

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in item:
            rich_issue['project'] = item['project']

        rich_issue['time_to_first_attention'] = None
        if issue['comments'] != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(str_to_datetime(issue['created_at']),
                                   self.get_time_to_first_attention(issue))
            rich_issue['num_of_comments_without_bot'] = \
                self.get_num_of_comments_without_bot(issue)
            rich_issue['time_to_first_attention_without_bot'] = \
                get_time_diff_days(str_to_datetime(issue['created_at']),
                                   self.get_time_to_first_attention_without_bot(issue))

        cve_message = self.get_CVE_message(issue)

        if cve_message:
            try:
                scores = cve_message['BaseScore'].split(' ')
                rich_issue['cve_public_time'] = cve_message['漏洞公开时间']
                rich_issue['cve_create_time'] = rich_issue['created_at']
                rich_issue['cve_percerving_time'] = rich_issue['time_to_first_attention_without_bot'] if 'time_to_first_attention_without_bot' in rich_issue else None
                rich_issue['cve_handling_time'] = rich_issue['time_open_days']
                if len(scores) == 2:
                    rich_issue['cve_base_score'] = scores[0]
                    rich_issue['cve_level'] = scores[1]
                else:
                    rich_issue['cve_base_score'] = None
                    rich_issue['cve_level'] = None
            except Exception as error:
                logger.error("CVE messgae is not complete: %s", error)

        else:
            rich_issue['cve_public_time'] = None
            rich_issue['cve_create_time'] = None
            rich_issue['cve_base_score'] = None
            rich_issue['cve_level'] = None
            rich_issue['cve_percerving_time'] = None
            rich_issue['cve_handling_time'] = None

        rich_issue.update(self.get_grimoire_fields(issue['created_at'], "issue"))

        item[self.get_field_date()] = rich_issue[self.get_field_date()]
        rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue

    def __get_rich_repo(self, item):
        rich_repo = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_repo[f] = item[f]
            else:
                rich_repo[f] = None

        repo = item['data']

        rich_repo['forks_count'] = repo['forks_count']
        rich_repo['subscribers_count'] = repo['watchers_count']
        rich_repo['stargazers_count'] = repo['stargazers_count']
        rich_repo['fetched_on'] = repo['fetched_on']
        rich_repo['url'] = repo['html_url']

        if self.prjs_map:
            rich_repo.update(self.get_item_project(rich_repo))

        rich_repo.update(self.get_grimoire_fields(item['metadata__updated_on'], "repository"))

        return rich_repo

    def enrich_onion(self, ocean_backend, enrich_backend,
                     in_index, out_index, data_source=None, no_incremental=False,
                     contribs_field='uuid',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp',
                     seconds=Enrich.ONION_INTERVAL):

        if not data_source:
            raise ELKError(cause="Missing data_source attribute")

        if data_source not in [GITEE_ISSUES, GITEE_MERGES, ]:
            logger.warning("[gitee] data source value {} should be: {} or {}".format(
                data_source, GITEE_ISSUES, GITEE_MERGES))

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index,
                             out_index=out_index,
                             data_source=data_source,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)

    def enrich_ossf_activity(self, ocean_backend, enrich_backend, out_index, git_demo_enriched_index, gitee_issues_enrich_index, gitee_pulls_enriched_index,
                             gitee_repositories_raw_index, from_date, end_date,probabilities=[0.5, 0.7, 0.9], interval_months=3,
                             date_field="metadata__updated_on"):

        logger.info("[enrich-ossf-activity] Start study")
        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index

        unique_repos = es_in.search(
            index=in_index,
            body=get_unique_repository())

        repositories = [repo['key'] for repo in unique_repos['aggregations']['unique_repos'].get('buckets', [])]
        # current_month = datetime_utcnow().replace(day=1, hour=0, minute=0, second=0)

        logger.info("[enrich-ossf-activity] {} repositories to process".format(len(repositories)))
        es_out = ElasticSearch(enrich_backend.elastic.url, out_index)
        es_out.add_alias("ossf_activity_study")

        num_items = 0
        ins_items = 0

        # iterate over the repositories
        for repository_url in repositories:
            logger.debug("[enrich-ossf-activity] Start analysis for {}".format(repository_url))
            data_list = self.get_date_list(datetime_to_utc(str_to_datetime(from_date)),datetime_to_utc(str_to_datetime(end_date)))
            activity_datas = []

          
            query_created_since = self.get_created_since_query(repository_url)
            gitee_create_since = es_in.search(index=gitee_repositories_raw_index, body=query_created_since)['hits']['hits']
            query_first_commit_since = self.get_created_since_query(repository_url+".git", order="asc")
            first_commit_since= es_in.search(index=git_demo_enriched_index, body=query_first_commit_since)['hits']['hits']
            
            creation_since = min(gitee_create_since[0]['_source']['data']["created_at"], first_commit_since[0]['_source']["metadata__updated_on"])
           
               
            
            for date in data_list:
                uuid_date = uuid(repository_url, str(date))             
                query_code_review_count = self.get_data_before_dates(repository_url, date)
                gitee_pr = es_in.search(index=gitee_pulls_enriched_index, body=query_code_review_count)['hits']['hits'] 
                code_review_count = sum(gitee_ipr['_source']['num_review_comments'] for gitee_ipr in gitee_pr)

                 
                query_updated_since = self.get_updated_since_query(repository_url+'.git', date)
                gitee_updated_since = es_in.search(index=git_demo_enriched_index, body=query_updated_since)['hits']['hits']
                
                created_since = get_time_diff_days(creation_since, str(date))
                
                query_author_uuid_data = self.get_uuid_count(repository_url, "author_uuid", to_date=date)
                author_uuid_count = es_in.search(index=(git_demo_enriched_index,gitee_pulls_enriched_index,gitee_issues_enrich_index), body=query_author_uuid_data)['aggregations']["count_of_uuid"]['value']
                query_assignee_uuid_data = self.get_uuid_count(repository_url, "assignee_data_uuid", to_date=date)
                assignee_data_uuid_count = es_in.search(index=gitee_issues_enrich_index, body=query_assignee_uuid_data)['aggregations']["count_of_uuid"]['value']
                query_merge_uuid_data = self.get_uuid_count(gitee_pulls_enriched_index, "merge_author_login", to_date=date)
                merge_login_count = es_in.search(index=gitee_issues_enrich_index, body=query_merge_uuid_data)['aggregations']["count_of_uuid"]['value']
                countributor_count = author_uuid_count+assignee_data_uuid_count+merge_login_count

                query_issue_closed = self.get_issue_closes_uuid_count(repository_url, "uuid", to_date=date)
                issue_closed = es_in.search(index=gitee_issues_enrich_index, body=query_issue_closed)['aggregations']["count_of_uuid"]['value']

                # query_issue_updated_since = self.get_updated_since_query(repository_url, date)
                # updated_issues_count = es_in.search(index=gitee_issues_enrich_index, body=query_issue_updated_since)['hits']['total']['value']
                query_issue_updated_since = self.get_uuid_count(repository_url, "uuid", to_date = date)
                updated_issues_count = es_in.search(index=gitee_issues_enrich_index, body=query_issue_updated_since)['aggregations']["count_of_uuid"]['value']

                if gitee_updated_since:
                    updated_since = get_time_diff_days(gitee_updated_since[0]['_source']["metadata__updated_on"], str(date))
                    # updated_issues_count = gitee_issue_updated_since
                    
                else:
                    continue
                activity_data = {
                    'uuid': uuid_date,
                    'repo':repository_url,
                    'code_review_count': code_review_count,
                    'created_since':created_since,
                    'updated_since':updated_since,
                    'countributor_count':countributor_count,
                    'updated_issues_count':updated_issues_count,
                    'closed_issue_count':issue_closed,
                    'grimoire_creation_date': date.isoformat(),                                  
                    'metadata__enriched_on': datetime_utcnow().isoformat()
                }
               
                if created_since>0:
                    activity_datas.append(activity_data)
                if len(activity_datas) >= self.elastic.max_items_bulk:
                        num_items += len(activity_datas)
                        ins_items += es_out.bulk_upload(activity_datas, "uuid")
                        activity_datas = []
        if len(activity_datas) > 0:
            num_items += len(activity_datas)
            ins_items += es_out.bulk_upload(activity_datas, "uuid")
        

        logger.info("[enrich-forecast-activity] End study")

    def get_date_list(self, begin_date, end_date):
        date_list = [x for x in list(pd.date_range(freq='7D', start=begin_date, end=end_date))]
        return date_list

    def get_data_before_dates(self, repository_url, date):
        query = """
        {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "origin": "%s"
                            }
                        },
                        {
                            "range": {
                                "metadata__updated_on": {
                                    "gte": "%s",
                                    "lt": "%s"
                                }
                            }
                        }
                    ]
                }
            }
            }
        """ % (repository_url,(date - timedelta(days = 90)).strftime("%Y-%m-%d"), date.strftime('%Y-%m-%d'))
        return query  

    def get_created_since_query(self, repository_url, order="desc"):
        query = """
        {
            "query": {
                "match": {
                "tag": "%s"
                }
            },
             "sort": [
            {
            "metadata__updated_on": { "order": "%s"}
            }
        ]
        }  
        """ % (repository_url, order)
        return query


    def get_updated_since_query(self, repository_url, date):
        query = """
      {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "origin": "%s"
                            }
                        },
                        {
                            "range": {
                                "metadata__updated_on": {
                                    
                                    "lt": "%s"
                                }
                            }
                        }
                    ]
                }
            },
             "sort": [
            {
            "metadata__updated_on": { "order": "desc"}
            }]
        }
        """ % (repository_url, date.strftime("%Y-%m-%d"))
        return query

    def get_uuid_count(self, repo_url, field, from_date=str_to_datetime("1970-01-01"), to_date= datetime_utcnow()):
        query = """
            {
                "size" : 0,
                "aggs" : {
                    "count_of_uuid" : {
                        "cardinality" : {
                        "field" : "%s"
                        }
                    }
                },
                "query": {
                "bool": { 
                "must": {
                "simple_query_string": {
                        "query": "%s*",
                        "fields": [
                            "origin"
                        ]
                        }
            },
            "filter": {
                    "range": { "grimoire_creation_date": { "gte": "%s" ,"lt": "%s"}} 
                    }}
            
            }
                
            }
        """% (field, repo_url, from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))
           
        return query

    def get_issue_closes_uuid_count(self, repo_url, field, from_date=str_to_datetime("1970-01-01"), to_date= datetime_utcnow()):
        query = """
        {
        "size": 0,
        "aggs": {
            "count_of_uuid": {
            "cardinality": {
                "field": "%s"
            }
            }
        },
        "query": {
            "bool": {
            "must": {
                "simple_query_string": {
                "query": "%s",
                "fields": [
                    "origin"
                ]
                }
            },
            "should": [
                {"term":{"state": "closed"}},
                {"term":{"state": "rejected"}}
            ]
            
            ,
            "filter": {
                "range": {
                "closed_at": {
                    "gte": "%s",
                    "lt": "%s"
                }
                }
            }
            }
        }
        }
        """%(field, repo_url, from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))
        return query
