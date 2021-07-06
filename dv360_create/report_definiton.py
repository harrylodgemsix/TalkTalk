import datetime
from datetime import date, timedelta


def unix_time_millis(dt, epoch):
    return (dt - epoch).total_seconds() * 1000.0


def yesterday_date():
    epoch = datetime.datetime.utcfromtimestamp(0)
    report_date = date.today() - timedelta(days=1)
    report_datetime = datetime.datetime.combine(
        report_date, datetime.time.min)
    report_date_ms_since_epoch = unix_time_millis(report_datetime, epoch)
    return report_date_ms_since_epoch


def filter_advertiser(list):
    # generates a list of advertisers to be used in report definition filter

    filter_advertiser = []
    for i in list:
        filter = {
            "type": "FILTER_ADVERTISER",
                    "value": i
        }
        filter_advertiser.append(filter)

    return filter_advertiser


def report_definition_generator(advertiser_list):
    # generates a report definition which will be sent to dv360

    report_definition = {
        "reportDataStartTimeMs": yesterday_date(),
        "reportDataEndTimeMs": yesterday_date(),
        "kind": "doubleclickbidmanager#query",
        "metadata": {
            "dataRange": "CUSTOM_DATES",
            "format": "CSV",
            "locale": "en",
            "title": "Bridgestone_DV360_report"
        },
        "params": {
            "filters": filter_advertiser(advertiser_list),
            "metrics": [
                "METRIC_TRUEVIEW_VIEWS",
                "METRIC_TRUEVIEW_VIEW_RATE",
                "METRIC_REVENUE_ADVERTISER",
                "METRIC_MEDIA_COST_ADVERTISER",
                "METRIC_CLICKS",
                "METRIC_IMPRESSIONS",
                "METRIC_TRUEVIEW_EARNED_LIKES",
                "METRIC_TRUEVIEW_EARNED_PLAYLIST_ADDITIONS",
                "METRIC_TRUEVIEW_EARNED_SHARES",
                "METRIC_TRUEVIEW_EARNED_SUBSCRIBERS",
                "METRIC_TRUEVIEW_EARNED_VIEWS",
                "METRIC_RICH_MEDIA_VIDEO_FIRST_QUARTILE_COMPLETES",
                "METRIC_RICH_MEDIA_VIDEO_THIRD_QUARTILE_COMPLETES",
                "METRIC_RICH_MEDIA_VIDEO_MIDPOINTS",
                "METRIC_RICH_MEDIA_VIDEO_COMPLETIONS",
                "METRIC_TOTAL_MEDIA_COST_ADVERTISER",
                "METRIC_TRACKED_ADS",
                "METRIC_ACTIVE_VIEW_PCT_MEASURABLE_IMPRESSIONS",
                "METRIC_ACTIVE_VIEW_PCT_VIEWABLE_IMPRESSIONS",
                "METRIC_ACTIVE_VIEW_ELIGIBLE_IMPRESSIONS",
                "METRIC_ACTIVE_VIEW_DISTRIBUTION_UNMEASURABLE",
                "METRIC_ACTIVE_VIEW_DISTRIBUTION_UNVIEWABLE",
                "METRIC_ACTIVE_VIEW_DISTRIBUTION_VIEWABLE",
                "METRIC_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS",
                "METRIC_ACTIVE_VIEW_UNMEASURABLE_IMPRESSIONS",
                "METRIC_ACTIVE_VIEW_UNVIEWABLE_IMPRESSIONS",
                "METRIC_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS",
                "METRIC_REVENUE_VIEWABLE_ECPM_ADVERTISER",
                "METRIC_PROFIT_ADVERTISER",
                "METRIC_PROFIT_VIEWABLE_ECPM_ADVERTISER",
                "METRIC_PROFIT_MARGIN",
                "METRIC_REVENUE_ECPM_ADVERTISER",
                "METRIC_REVENUE_ECPC_ADVERTISER",
                "METRIC_MEDIA_COST_ECPC_ADVERTISER",
                "METRIC_MEDIA_COST_ECPM_ADVERTISER",
                "METRIC_MEDIA_COST_VIEWABLE_ECPM_ADVERTISER",
                "METRIC_TOTAL_MEDIA_COST_ECPC_ADVERTISER",
                "METRIC_TOTAL_MEDIA_COST_ECPM_ADVERTISER",

            ],
            "groupBys": [
                "FILTER_DATE",
                "FILTER_PARTNER",
                "FILTER_PARTNER_NAME",
                "FILTER_ADVERTISER",
                "FILTER_ADVERTISER_NAME",
                "FILTER_ADVERTISER_CURRENCY",
                "FILTER_INSERTION_ORDER",
                "FILTER_INSERTION_ORDER_NAME",
                "FILTER_DEVICE_TYPE",
                "FILTER_LINE_ITEM",
                "FILTER_LINE_ITEM_NAME",
                "FILTER_TRUEVIEW_AD_GROUP",
                "FILTER_TRUEVIEW_AD_GROUP_ID"
            ],
            "type": "TYPE_TRUEVIEW"
        },
        "schedule": {
            "frequency": "ONE_TIME",
            "nextRunTimezoneCode": "Europe/London"
        },
        "timezoneCode": "Europe/London"
    }

    return report_definition
