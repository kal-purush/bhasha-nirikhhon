from .models import Category, Article, Tag, BlogSettings
from django.conf import settings
from django.db.models import Q
from Jin_Blog.utils import get_blog_setting
from django.utils import timezone
import logging
logger = logging.getLogger(__name__)

def processor(requests):
	logger.info('set processor')
	setting = get_blog_setting()
	
	today = timezone.now().strftime("%Y-%m-%d 00:00:00") 
	today_end = timezone.now().strftime("%Y-%m-%d 23:59:59") 
	
	value = {
		'SITE_NAME': setting.sitename,
		'SHOW_GOOGLE_ADSENSE': setting.show_google_adsense,
		'GOOGLE_ADSENSE_CODES': setting.google_adsense_codes,
		'SITE_DESCRIPTION': setting.site_description,
		'SITE_KEYWORDS': setting.site_keywords,
		'SITE_BASE_URL': requests.scheme + '://' + requests.get_host() + '/',
		'nav_category_list': Category.objects.all(),
		'nav_pages': Article.objects.filter(Q(pub_time__range=[today, today_end]) | Q(type='p')),
		'DATE_TODAY' : today,
#		'OPEN_SITE_COMMENT': setting.open_site_comment,
		'ANALYTICS_CODE': setting.analyticscode,
		'most_read_articles': Article.objects.filter(status = 'p').order_by('-views')[:setting.sidebar_article_count],
		'recent_articles': Article.objects.filter(status='p')[:setting.sidebar_article_count],
	}

	return value