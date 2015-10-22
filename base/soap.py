"""
(c) 2013 LinkedIn Corp. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");?you may not use this file except in compliance with the License. You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software?distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""
import logging

from lxml import etree
from lxml.builder import ElementMaker
from datetime import datetime
from pytz import utc, timezone

from ..exceptions import FailedExchangeException

SOAP_NS = u'http://schemas.xmlsoap.org/soap/envelope/'
TYPE_NS = u'http://schemas.microsoft.com/exchange/services/2006/types'

SOAP_NAMESPACES = {u't': TYPE_NS, u's': SOAP_NS}
S = ElementMaker(namespace=SOAP_NS, nsmap=SOAP_NAMESPACES)
T = ElementMaker(namespace=TYPE_NS, nsmap=SOAP_NAMESPACES)

log = logging.getLogger('pyexchange')


class ExchangeServiceSOAP(object):

  EXCHANGE_DATE_FORMAT = u"%Y-%m-%dT%H:%M:%SZ"

  def __init__(self, connection):
    self.connection = connection

  def send(self, xml, headers=None, retries=4, timeout=30, encoding="utf-8", user_availability_req=False):
    request_xml = self._wrap_soap_xml_request(xml, user_availability_req)
    log.info(etree.tostring(request_xml, encoding=encoding, pretty_print=True))
    response = self._send_soap_request(request_xml, headers=headers, retries=retries, timeout=timeout, encoding=encoding)
    return self._parse(response, encoding=encoding)

  def _parse(self, response, encoding="utf-8"):

    try:
      tree = etree.XML(response.encode(encoding))
    except (etree.XMLSyntaxError, TypeError) as err:
      raise FailedExchangeException(u"Unable to parse response from Exchange - check your login information. Error: %s" % err)

    self._check_for_errors(tree)

    log.info(etree.tostring(tree, encoding=encoding, pretty_print=True))
    return tree

  def _check_for_errors(self, xml_tree):
    self._check_for_SOAP_fault(xml_tree)

  def _check_for_SOAP_fault(self, xml_tree):
    # Check for SOAP errors. if <soap:Fault> is anywhere in the response, flip out

    fault_nodes = xml_tree.xpath(u'//s:Fault', namespaces=SOAP_NAMESPACES)

    if fault_nodes:
      fault = fault_nodes[0]
      log.debug(etree.tostring(fault, pretty_print=True))
      raise FailedExchangeException(u"SOAP Fault from Exchange server", fault.text)

  def _send_soap_request(self, xml, headers=None, retries=2, timeout=30, encoding="utf-8"):
    body = etree.tostring(xml, encoding=encoding)
    response = self.connection.send(body, headers, retries, timeout)
    return response

  def _exchange_header(self, user_availability_req):
      if user_availability_req:
          return user_availability_header()
      else:
          return S.Header(T.RequestServerVersion({u'Version': u'Exchange2010'}))

  def _wrap_soap_xml_request(self, exchange_xml, user_availability_req):
    root = S.Envelope(self._exchange_header(user_availability_req), S.Body(exchange_xml))
    return root

  def _parse_date(self, date_string):
    date = datetime.strptime(date_string, self.EXCHANGE_DATE_FORMAT)
    date = date.replace(tzinfo=utc)

    return date

  def _parse_date_only_naive(self, date_string):
    date = datetime.strptime(date_string[0:10], self.EXCHANGE_DATE_FORMAT[0:8])

    return date.date()

  def _xpath_to_dict(self, element, property_map, namespace_map):
    """
    property_map = {
      u'name'         : { u'xpath' : u't:Mailbox/t:Name'},
      u'email'        : { u'xpath' : u't:Mailbox/t:EmailAddress'},
      u'response'     : { u'xpath' : u't:ResponseType'},
      u'last_response': { u'xpath' : u't:LastResponseTime', u'cast': u'datetime'},
    }

    This runs the given xpath on the node and returns a dictionary

    """

    result = {}

    log.info(etree.tostring(element, pretty_print=True))

    for key in property_map:
      item = property_map[key]
      log.info(u'Pulling xpath {xpath} into key {key}'.format(key=key, xpath=item[u'xpath']))
      nodes = element.xpath(item[u'xpath'], namespaces=namespace_map)

      if nodes:
        result_for_node = []

        for node in nodes:
          cast_as = item.get(u'cast', None)

          if cast_as == u'datetime':
            result_for_node.append(self._parse_date(node.text))
          elif cast_as == u'date_only_naive':
            result_for_node.append(self._parse_date_only_naive(node.text))
          elif cast_as == u'int':
            result_for_node.append(int(node.text))
          elif cast_as == u'bool':
            if node.text.lower() == u'true':
              result_for_node.append(True)
            else:
              result_for_node.append(False)
          else:
            result_for_node.append(node.text)

        if not result_for_node:
          result[key] = None
        elif len(result_for_node) == 1:
          result[key] = result_for_node[0]
        else:
          result[key] = result_for_node

    return result

def user_availability_header():
    """
      <soap:Header>
        <t:RequestServerVersion Version="Exchange2010" />
        <t:TimeZoneContext>
          <t:TimeZoneDefinition Name="(UTC-08:00) Pacific Time (US &amp; Canada)" Id="Pacific Standard Time">
            <t:Periods>
              <t:Period Bias="P0DT8H0M0.0S" Name="Standard" Id="Std" />
              <t:Period Bias="P0DT7H0M0.0S" Name="Daylight" Id="Dlt/1" />
              <t:Period Bias="P0DT7H0M0.0S" Name="Daylight" Id="Dlt/2007" />
            </t:Periods>
            <t:TransitionsGroups>
              <t:TransitionsGroup Id="0">
                <t:RecurringDayTransition>
                  <t:To Kind="Period">Dlt/1</t:To>
                  <t:TimeOffset>P0DT2H0M0.0S</t:TimeOffset>
                  <t:Month>4</t:Month>
                  <t:DayOfWeek>Sunday</t:DayOfWeek>
                  <t:Occurrence>1</t:Occurrence>
                </t:RecurringDayTransition>
                <t:RecurringDayTransition>
                  <t:To Kind="Period">Std</t:To>
                  <t:TimeOffset>P0DT2H0M0.0S</t:TimeOffset>
                  <t:Month>10</t:Month>
                  <t:DayOfWeek>Sunday</t:DayOfWeek>
                  <t:Occurrence>-1</t:Occurrence>
                </t:RecurringDayTransition>
              </t:TransitionsGroup>
              <t:TransitionsGroup Id="1">
                <t:RecurringDayTransition>
                  <t:To Kind="Period">Dlt/2007</t:To>
                  <t:TimeOffset>P0DT2H0M0.0S</t:TimeOffset>
                  <t:Month>3</t:Month>
                  <t:DayOfWeek>Sunday</t:DayOfWeek>
                  <t:Occurrence>2</t:Occurrence>
                </t:RecurringDayTransition>
                <t:RecurringDayTransition>
                  <t:To Kind="Period">Std</t:To>
                  <t:TimeOffset>P0DT2H0M0.0S</t:TimeOffset>
                  <t:Month>11</t:Month>
                  <t:DayOfWeek>Sunday</t:DayOfWeek>
                  <t:Occurrence>1</t:Occurrence>
                </t:RecurringDayTransition>
              </t:TransitionsGroup>
            </t:TransitionsGroups>
            <t:Transitions>
              <t:Transition>
                <t:To Kind="Group">0</t:To>
              </t:Transition>
              <t:AbsoluteDateTransition>
                <t:To Kind="Group">1</t:To>
                <t:DateTime>2007-01-01T08:00:00.000Z</t:DateTime>
              </t:AbsoluteDateTransition>
            </t:Transitions>
          </t:TimeZoneDefinition>
        </t:TimeZoneContext>
      </soap:Header>
    """

    header = S.Header(
                T.RequestServerVersion({u'Version': u'Exchange2010'}),
                T.TimeZoneContext(
                    T.TimeZoneDefinition({
                        u'Name': u'(UTC-08:00) Pacific Time (US &amp; Canada)',
                        u'Id': u'Pacific Standard Time',
                        },
                        T.Periods(
                            T.Period({
                                u'Bias':u'P0DT8H0M0.0S',
                                u'Name':u'Standard',
                                u'Id'  :u'Std',
                            }),
                            T.Period({
                                u'Bias':u'P0DT7H0M0.0S',
                                u'Name':u'Daylight',
                                u'Id'  :u'Dlt/1',
                            }),
                        ),
                        T.TransitionsGroups(
                            T.TransitionsGroup(
                                {u'Id':u'0'},
                                T.RecurringDayTransition(
                                    T.To({u'Kind':u'Period'}, u'Dlt/1'),
                                    T.TimeOffset(u'P0DT2H0M0.0S'),
                                    T.Month(u'3'),
                                    T.DayOfWeek(u'Sunday'),
                                    T.Occurrence(u'1')
                                ),
                                T.RecurringDayTransition(
                                    T.To({u'Kind':u'Period'}, u'Std'),
                                    T.TimeOffset(u'P0DT2H0M0.0S'),
                                    T.Month(u'11'),
                                    T.DayOfWeek(u'Sunday'),
                                    T.Occurrence(u'1')
                                )
                            )
                        ),
                        T.Transitions(
                            T.Transition(
                                T.To({u'Kind':u'Group'}, u'0')
                            ),
                            T.AbsoluteDateTransition(
                                T.To({u'Kind':u'Group'}, u'0'),
                                T.DateTime(u'2015-03-01T08:00:00.000Z')
                            )
                        )
                    )
                 )
              )

    return header
