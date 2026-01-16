const trim = (strings: TemplateStringsArray) => {
	return strings.join('\n').trim();
};

export const allRooms = trim`
select
	r.id,
	group_concat(u.nickname) as current_users,
	r3.max_users,
	r.removed_at is null as active,
	strftime(
		'%H:%M:%S',
		case
			when r.removed_at is null then
				julianday('now') - julianday(r.created_at)
			else
				julianday(r.removed_at) - julianday(r.created_at)
		end,
		'12:00'
	) as time_active
from
	room as r
left
	join
		room_state as s
	on
		r.id = s.room_id
	and
		s.created_at = (
			select
				max(created_at)
			from
				room_state
			where
				r.id = room_id
		)
left
	join
		room_state_user as u
	on
		u.room_state_id = s.id
left
	join
		(
			select
				id,
				max(num) as max_users
			from
				(
					select
						r2.id,
						count(s2.id) as num
					from
						room r2
					left
						join
							room_state s2
						on
							s2.room_id = r2.id
					left
						join
							room_state_user u2
						on
							u2.room_state_id = s2.id
					group by
						s2.id
				)
			group by
				id
		) as r3
	on
		r3.id = r.id
group by
	r.id
order by
	active desc,
	time_active desc
`;

export const activeRooms = trim`
select
	r.id,
	group_concat(u.nickname) as current_users,
	r3.max_users,
	strftime(
		'%H:%M:%S',
		julianday('now') - julianday(r.created_at),
		'12:00'
	) as time_active
from
	room as r
left
	join
		room_state as s
	on
		r.id = s.room_id
	and
		s.created_at = (
			select
				max(created_at)
			from
				room_state
			where
				r.id = room_id
		)
left
	join
		room_state_user as u
	on
		u.room_state_id = s.id
left
	join
		(
			select
				id,
				max(num) as max_users
			from
				(
					select
						r2.id,
						count(s2.id) as num
					from
						room r2
					left
						join
							room_state s2
						on
							s2.room_id = r2.id
					left
						join
							room_state_user u2
						on
							u2.room_state_id = s2.id
					group by
						s2.id
				)
			group by
				id
		) as r3
	on
		r3.id = r.id
where
	r.removed_at is null
group by
	r.id
order by
	time_active desc
`;