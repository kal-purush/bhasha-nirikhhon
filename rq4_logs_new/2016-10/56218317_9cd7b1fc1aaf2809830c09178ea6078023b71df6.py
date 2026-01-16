# -*- coding: utf-8 -*-
from south.utils import datetime_utils as datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'CollectNews'
        db.create_table(u'news_collectnews', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('user', self.gf('django.db.models.fields.related.OneToOneField')(related_name='user_collect_news', unique=True, to=orm['account.User'])),
        ))
        db.send_create_signal(u'news', ['CollectNews'])

        # Adding M2M table for field news on 'CollectNews'
        m2m_table_name = db.shorten_name(u'news_collectnews_news')
        db.create_table(m2m_table_name, (
            ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True)),
            ('collectnews', models.ForeignKey(orm[u'news.collectnews'], null=False)),
            ('news', models.ForeignKey(orm[u'news.news'], null=False))
        ))
        db.create_unique(m2m_table_name, ['collectnews_id', 'news_id'])


    def backwards(self, orm):
        # Deleting model 'CollectNews'
        db.delete_table(u'news_collectnews')

        # Removing M2M table for field news on 'CollectNews'
        db.delete_table(db.shorten_name(u'news_collectnews_news'))


    models = {
        u'account.user': {
            'Meta': {'object_name': 'User'},
            'date_joined': ('django.db.models.fields.DateTimeField', [], {'default': 'datetime.datetime.now'}),
            'email': ('django.db.models.fields.EmailField', [], {'max_length': '75', 'blank': 'True'}),
            'first_name': ('django.db.models.fields.CharField', [], {'max_length': '30', 'blank': 'True'}),
            'groups': ('django.db.models.fields.related.ManyToManyField', [], {'symmetrical': 'False', 'related_name': "u'user_set'", 'blank': 'True', 'to': u"orm['auth.Group']"}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'is_active': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            'is_staff': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'is_superuser': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'last_login': ('django.db.models.fields.DateTimeField', [], {'default': 'datetime.datetime.now'}),
            'last_name': ('django.db.models.fields.CharField', [], {'max_length': '30', 'blank': 'True'}),
            'nickname': ('django.db.models.fields.CharField', [], {'default': "''", 'max_length': '30', 'null': 'True', 'blank': 'True'}),
            'password': ('django.db.models.fields.CharField', [], {'max_length': '128'}),
            'phone_number': ('django.db.models.fields.CharField', [], {'default': "''", 'max_length': '30', 'null': 'True', 'blank': 'True'}),
            'photo': ('django.db.models.fields.files.ImageField', [], {'max_length': '255', 'null': 'True', 'blank': 'True'}),
            'user_permissions': ('django.db.models.fields.related.ManyToManyField', [], {'symmetrical': 'False', 'related_name': "u'user_set'", 'blank': 'True', 'to': u"orm['auth.Permission']"}),
            'username': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '30'})
        },
        u'auth.group': {
            'Meta': {'object_name': 'Group'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '80'}),
            'permissions': ('django.db.models.fields.related.ManyToManyField', [], {'to': u"orm['auth.Permission']", 'symmetrical': 'False', 'blank': 'True'})
        },
        u'auth.permission': {
            'Meta': {'ordering': "(u'content_type__app_label', u'content_type__model', u'codename')", 'unique_together': "((u'content_type', u'codename'),)", 'object_name': 'Permission'},
            'codename': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'content_type': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['contenttypes.ContentType']"}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '50'})
        },
        u'contenttypes.contenttype': {
            'Meta': {'ordering': "('name',)", 'unique_together': "(('app_label', 'model'),)", 'object_name': 'ContentType', 'db_table': "'django_content_type'"},
            'app_label': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'model': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '100'})
        },
        u'news.aws': {
            'Meta': {'object_name': 'AWS'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'key_id': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            's_key': ('django.db.models.fields.CharField', [], {'max_length': '100'})
        },
        u'news.collectnews': {
            'Meta': {'object_name': 'CollectNews'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'news': ('django.db.models.fields.related.ManyToManyField', [], {'related_name': "'collect_news'", 'symmetrical': 'False', 'to': u"orm['news.News']"}),
            'user': ('django.db.models.fields.related.OneToOneField', [], {'related_name': "'user_collect_news'", 'unique': 'True', 'to': u"orm['account.User']"})
        },
        u'news.news': {
            'Meta': {'object_name': 'News'},
            'content': ('django.db.models.fields.CharField', [], {'max_length': '8000'}),
            'description': ('django.db.models.fields.CharField', [], {'max_length': '4096'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'link': ('django.db.models.fields.CharField', [], {'max_length': '1024'}),
            'news_id': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'pubdate': ('django.db.models.fields.DateTimeField', [], {}),
            'rev': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'title': ('django.db.models.fields.CharField', [], {'max_length': '1024'}),
            'updated': ('django.db.models.fields.DateTimeField', [], {'auto_now_add': 'True', 'blank': 'True'})
        },
        u'news.newsaudio': {
            'Meta': {'object_name': 'NewsAudio'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'news': ('django.db.models.fields.related.OneToOneField', [], {'blank': 'True', 'related_name': "'news_video'", 'unique': 'True', 'null': 'True', 'to': u"orm['news.News']"}),
            'path': ('django.db.models.fields.CharField', [], {'max_length': '1024'}),
            'source': ('django.db.models.fields.CharField', [], {'max_length': '400'})
        },
        u'news.newsphoto': {
            'Meta': {'object_name': 'NewsPhoto'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'news': ('django.db.models.fields.related.OneToOneField', [], {'blank': 'True', 'related_name': "'news_photo'", 'unique': 'True', 'null': 'True', 'to': u"orm['news.News']"}),
            'path': ('django.db.models.fields.CharField', [], {'max_length': '1024'}),
            'source': ('django.db.models.fields.CharField', [], {'max_length': '400'})
        }
    }

    complete_apps = ['news']