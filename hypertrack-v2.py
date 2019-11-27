#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json, os, time, requests 

# Query Pinot - list of orders
def failed_orders_in_time(query_format, region_ids, port_num, time_threshold_in_day):
	current_timestamp_ms = int(time.time()) * 1000

	#  calculate time_back using desired time_shreshold_in_day
	time_back = current_timestamp_ms - (time_threshold_in_day * 24 * 60 * 60 * 1000)

	# format the query with region_ids and time_back
	query = query_format % (region_ids, str(time_back))
	
	# format the url with port number and query
	url = 'http://localhost:%s/%s' % (port_num, query)
	request = requests.post(url, data=json.dumps({'pql': query}))
	response = json.loads(request.text)

	# Convert the response into a proper dict
	failed_orders = []
	column_names = response['selectionResults']['columns']

	for order in response['selectionResults']['results']:
		order_detail = {}

		for x in range(0, len(order)):
			order_detail[column_names[x]] = order[x]

		failed_orders.append(order_detail)

	return failed_orders


# Unique couriers from failed orders
def unique_couriers_failing_orders(failed_orders):
	couriers = {}
	courier_to_city_mapping = {}

	for order in failed_orders:
		courier_uuid = order['courierUUID']
		rush_begun_at = str(order['rushBegunAt']) 
		city_id = order['regionId']

		# Update the courier to city map (for forcing offline)
		courier_to_city_mapping[courier_uuid] = city_id

		# Group failed orders by courier
		if courier_uuid not in couriers:
			couriers[courier_uuid] = [rush_begun_at]
		else:
			failed_orders = couriers[courier_uuid]
			failed_orders.append(rush_begun_at)
			couriers[courier_uuid] = failed_orders

	return couriers, courier_to_city_mapping


# get bad_couriers' total order count within time_threshold_in_day
def get_couriers_order_count(bad_couriers, region_ids, port_num, time_threshold_in_day):
	current_timestamp_ms = int(time.time()) * 1000
	time_back = current_timestamp_ms - (time_threshold_in_day * 24 * 60 * 60 * 1000)

	# If a courier picks up 2 orders from the same restaurant at same start timestamp, they are considered as one order
	query =
	'select courier_id, count(distinct rushAcceptedAt) \
	from eats_emea_hypertrack_all_orders_REALTIME \
	where regionId in (%s) \
	and courier_id in (%s) \
	and rushAcceptedAt > %s \
	group by courier_id' % (region_ids, ','.join(list(bad_couriers.keys())), str(time_back))

	# format the url with port number and query
	url = 'http://localhost:%s/%s' % (port_num, query)
	request = requests.post(url, data=json.dumps({'pql': query}))
	response = json.loads(request.text)

	# Convert the response into a couriers_order_count dict
	couriers_order_count = {}
	for courier_id, count in response['selectionResults']['results']:
		couriers_order_count[courier_id] = count

	return couriers_order_count


# Indentify bad couriers
def identify_really_bad_couriers(bad_couriers, couriers_order_count, fail_rate_threshold):
	really_bad_couriers = []

	for courier_uuid, begin_trip_timestamp in bad_couriers.iteritems():
		# If a courier picks up 2 orders from the same restaurant, they share the same start timestamp but we want to only penalise once
		unique_failed_trip_count = len(set(begin_trip_timestamp)) 
		if unique_failed_trip_count / couriers_order_count[courier_uuid] >= fail_rate_threshold:
			really_bad_couriers.append(courier_uuid)

	return really_bad_couriers


# Waitlist really bad couriers
def waitlist_really_bad_couriers(really_bad_couriers, courier_to_city_mapping):	
	accountManager = courier_account_manager.CourierAccountManager()

	for courierUUID in really_bad_couriers:
		
		# Identify the courier's city ID
		cityID = courier_to_city_mapping[courierUUID]

		print 'Waitlisting ' + courierUUID

		#
		#
		# Deactivation service logic here (leave as placeholder)
		#
		#

def excute_rule(rule):
	query_format = rule['query_format']
	region_ids = rule['region_ids']
	port_num = rule['port_num']
	time_threshold_in_day = rule['time_threshold_in_day']
	fail_rate_threshold = rule['fail_rate_threshold']

	# Query Pinot to identify all failed orders from the given time
	failed_orders = failed_orders_in_time(query_format, region_ids, port_num, time_threshold_in_day)

	# Group the output by courier uuid
	bad_couriers, courier_to_city_mapping = unique_couriers_failing_orders(failed_orders)

	# Get bad_couriers' count of total order using Query Pinot
	couriers_order_count = get_couriers_order_count(bad_couriers, region_ids, port_num, time_threshold_in_day)

	# validate really bad couriers with given fail_rate_threshold
	really_bad_couriers = identify_really_bad_couriers(bad_couriers, couriers_order_count, fail_rate_threshold)

	waitlist_really_bad_couriers(really_bad_couriers, courier_to_city_mapping)


if __name__ == "__main__":
	# fetch rules
	with open('rules.json') as json_file:
		rules = json.load(json_file)
		for rule in rules:
			excute_rule(rule)



