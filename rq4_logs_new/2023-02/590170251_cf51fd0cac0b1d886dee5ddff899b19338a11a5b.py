"""
Copyright ©2023. The Regents of the University of California (Regents). All Rights Reserved.

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
from ripley import db, std_commit
from ripley.lib.util import to_isoformat
from ripley.models.base import Base


class UserAuth(Base):
    __tablename__ = 'user_auths'

    id = db.Column(db.Integer, nullable=False, primary_key=True)  # noqa: A003
    active = db.Column(db.Boolean, nullable=False, default=False)
    is_canvas_whitelisted = db.Column(db.Boolean, nullable=False, default=False)
    is_superuser = db.Column(db.Boolean, nullable=False, default=False)
    uid = db.Column(db.String(255), nullable=False)

    def __init__(self, active, uid, is_canvas_whitelisted=False, is_superuser=False):
        self.active = active
        self.is_canvas_whitelisted = is_canvas_whitelisted
        self.is_superuser = is_superuser
        self.uid = uid

    def __repr__(self):
        return f"""<UserAuth
                    id={self.id},
                    active={self.active},
                    is_canvas_whitelisted={self.is_canvas_whitelisted},
                    is_superuser={self.is_superuser},
                    uid={self.uid},
                """

    @classmethod
    def create(cls, active, uid, is_canvas_whitelisted=False, is_superuser=False):
        user_auth = cls(
            active=active,
            is_canvas_whitelisted=is_canvas_whitelisted,
            is_superuser=is_superuser,
            uid=uid,
        )
        db.session.add(user_auth)
        std_commit()
        return user_auth

    @classmethod
    def find_by_id(cls, user_auth_id):
        return cls.query.filter_by(id=user_auth_id).first()

    @classmethod
    def find_by_uid(cls, uid):
        return cls.query.filter_by(uid=uid).first()

    def to_api_json(self):
        return {
            'id': self.id,
            'active': self.active,
            'createdAt': to_isoformat(self.created_at),
            'isCanvasWhitelisted': self.is_canvas_whitelisted,
            'isSuperuser': self.is_superuser,
            'uid': self.uid,
            'updatedAt': to_isoformat(self.updated_at),
        }