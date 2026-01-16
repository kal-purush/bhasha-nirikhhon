"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""
from flask import current_app as app
from ripley.lib.http import tolerant_jsonify


@app.route('/api/academics/canvas/course_provision')
def canvas_course_provision():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/external_tools')
def canvas_external_tools():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/site_creation/authorizations')
def canvas_site_creation():
    return tolerant_jsonify({
        'authorizations': {
            'canCreateCourseSite': True,
            'canCreateProjectSite': True,
        },
    })


@app.route('/api/academics/canvas/user_can_create_site')
def canvas_user_can_create_site():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/course_add_user/${canvasCourseId}/course_sections')
def canvas_course_add_user():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/course_add_user/${canvasCourseId}/search_users?searchText=${searchText}&searchType=${searchType}')
def canvas_course_search_users():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/course_provision/sections_feed/${canvasCourseId}')
def canvas_course_provision_sections_feed():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/course_provision/status?jobId=${jobId}')
def canvas_course_provision_status():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/course_user_roles/${canvasCourseId}')
def canvas_course_user_roles():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/egrade_export/options/${canvasCourseId}')
def canvas_egrade_export():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/egrade_export/status/${canvasCourseId}?jobId=${jobId}')
def canvas_egrade_export_status():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/mailing_list/${canvasCourseId}')
def canvas_mailing_list():
    return tolerant_jsonify([])


@app.route('/api/academics/canvas/mailing_lists/${canvasCourseId}')
def canvas_mailing_lists():
    return tolerant_jsonify([])


@app.route('/api/academics/rosters/canvas/${courseId}')
def canvas_rosters():
    return tolerant_jsonify([])


@app.route('/api/canvas/media/${canvasCourseId}')
def canvas_media():
    return tolerant_jsonify([])