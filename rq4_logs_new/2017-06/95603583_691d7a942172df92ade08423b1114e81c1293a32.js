/* global phpbb */

(function($) {  // Avoid conflicts with other libraries

'use strict';

// This callback will mark all forum icons read
phpbb.addAjaxCallback('mark_forums_read', function(res) {
	var readTitle = res.NO_UNREAD_POSTS;
	var unreadTitle = res.UNREAD_POSTS;
	var iconsArray = {
		forum_unread: 'forum_read',
		forum_unread_subforum: 'forum_read_subforum',
		forum_unread_locked: 'forum_read_locked'
	};

	$('li.row').find('dl[class*="forum_unread"]').each(function() {
		var $this = $(this);

		$.each(iconsArray, function(unreadClass, readClass) {
			if ($this.hasClass(unreadClass)) {
				$this.removeClass(unreadClass).addClass(readClass);
			}
		});
		$this.children('dt[title="' + unreadTitle + '"]').attr('title', readTitle);
	});

	// Mark subforums read
	$('a.subforum[class*="unread"]').removeClass('unread').addClass('read');

	// Mark topics read if we are watching a category and showing active topics
	if ($('#active_topics').length) {
		phpbb.ajaxCallbacks.mark_topics_read.call(this, res, false);
	}

	// Update mark forums read links
	$('[data-ajax="mark_forums_read"]').attr('href', res.U_MARK_FORUMS);

	phpbb.closeDarkenWrapper(3000);
});

/**
* This callback will mark all topic icons read
*
* @param {bool} [update_topic_links=true] Whether "Mark topics read" links
* 	should be updated. Defaults to true.
*/
phpbb.addAjaxCallback('mark_topics_read', function(res, updateTopicLinks) {
	var readTitle = res.NO_UNREAD_POSTS;
	var unreadTitle = res.UNREAD_POSTS;
	var iconsArray = {
		global_unread: 'global_read',
		announce_unread: 'announce_read',
		sticky_unread: 'sticky_read',
		topic_unread: 'topic_read'
	};
	var iconsState = ['', '_hot', '_hot_mine', '_locked', '_locked_mine', '_mine'];
	var unreadClassSelectors;
	var classMap = {};
	var classNames = [];

	if (typeof updateTopicLinks === 'undefined') {
		updateTopicLinks = true;
	}

	$.each(iconsArray, function(unreadClass, readClass) {
		$.each(iconsState, function(key, value) {
			// Only topics can be hot
			if ((value === '_hot' || value === '_hot_mine') && unreadClass !== 'topic_unread') {
				return true;
			}
			classMap[unreadClass + value] = readClass + value;
			classNames.push(unreadClass + value);
		});
	});

	unreadClassSelectors = '.' + classNames.join(',.');

	$('li.row').find(unreadClassSelectors).each(function() {
		var $this = $(this);
		$.each(classMap, function(unreadClass, readClass) {
			if ($this.hasClass(unreadClass)) {
				$this.removeClass(unreadClass).addClass(readClass);
			}
		});
		$this.children('dt[title="' + unreadTitle + '"]').attr('title', readTitle);
	});

	// Remove link to first unread post
	$('a.unread').has('.icon-red').remove();

	// Update mark topics read links
	if (updateTopicLinks) {
		$('[data-ajax="mark_topics_read"]').attr('href', res.U_MARK_TOPICS);
	}

	phpbb.closeDarkenWrapper(3000);
});

// This callback will mark all notifications read
phpbb.addAjaxCallback('notification.mark_all_read', function(res) {
	if (typeof res.success !== 'undefined') {
		phpbb.markNotifications($('#notification_list li.list-group-item-info, #notification_list_mobile li.list-group-item-info'), 0);
		phpbb.closeDarkenWrapper(3000);
	}
});

// This callback will mark a notification read
phpbb.addAjaxCallback('notification.mark_read', function(res) {
	if (typeof res.success !== 'undefined') {
		var unreadCount = Number($('#notification_list_button span').html()) - 1;
		phpbb.markNotifications($(this).parent('li.list-group-item-info'), unreadCount);
	}
});

/**
 * Mark notification popup rows as read.
 *
 * @param {jQuery} $popup jQuery object(s) to mark read.
 * @param {int} unreadCount The new unread notifications count.
 */
phpbb.markNotifications = function($popup, unreadCount) {
	// Remove the unread status.
	$popup.removeClass('list-group-item-info');
	$popup.find('a.mark_read').remove();

	// Update the notification link to the real URL.
	$popup.each(function() {
		var link = $(this).find('a');
		link.attr('href', link.attr('data-real-url'));
	});

	// Update the unread count.
	$('span', '#notification_list_button').html(unreadCount);
	// Remove the Mark all read link and hide notification count if there are no unread notifications.
	if (!unreadCount) {
		$('#notification_list .panel-body, #notification_list_mobile .panel-body').remove();
	}

	// Update page title
	var $title = $('title');
	var originalTitle = $title.text().replace(/(\((\d+)\))/, '');
	$title.text((unreadCount ? '(' + unreadCount + ')' : '') + originalTitle);
};

// This callback finds the post from the delete link, and removes it.
phpbb.addAjaxCallback('post_delete', function() {
	var $this = $(this),
		postId;

	if ($this.attr('data-refresh') === undefined) {
		postId = $this[0].href.split('&p=')[1];
		var post = $this.parents('#p' + postId).css('pointer-events', 'none');
		if (post.hasClass('bg1') || post.hasClass('bg2')) {
			var posts1 = post.nextAll('.bg1');
			post.nextAll('.bg2').removeClass('bg2').addClass('bg1');
			posts1.removeClass('bg1').addClass('bg2');
		}
		post.fadeOut(function() {
			$(this).remove();
		});
	}
});

// This callback removes the approve / disapprove div or link.
phpbb.addAjaxCallback('post_visibility', function(res) {
	var remove = (res.visible) ? $(this) : $(this).parents('.post');
	$(remove).css('pointer-events', 'none').fadeOut(function() {
		$(this).remove();
	});

	if (res.visible) {
		// Remove the "Deleted by" message from the post on restoring.
		remove.parents('.post').find('.post_deleted_msg').css('pointer-events', 'none').fadeOut(function() {
			$(this).remove();
		});
	}
});

// This removes the parent row of the link or form that fired the callback.
phpbb.addAjaxCallback('row_delete', function() {
	$(this).parents('tr').remove();
});

// This handles friend / foe additions removals.
phpbb.addAjaxCallback('zebra', function(res) {
	var zebra;

	if (res.success) {
		zebra = $('.zebra');
		zebra.first().html(res.MESSAGE_TEXT);
		zebra.not(':first').html('&nbsp;').prev().html('&nbsp;');
	}
});

/**
 * This callback updates the poll results after voting.
 */
phpbb.addAjaxCallback('vote_poll', function(res) {
	if (typeof res.success !== 'undefined') {
		var poll = $('.topic_poll');
		var panel = poll.find('.panel');
		var resultsVisible = poll.find('div:first-child .progress').is(':visible');
		var mostVotes = 0;

		// Remove the View results link
		if (!resultsVisible) {
			poll.find('.poll_view_results').hide(500);
		}

		if (!res.can_vote) {
			poll.find('.vote_option').each(function () {
				$(this).html($('.vote_caption').html());
			});
			$('.author_poll').hide(500);
			$('.submit_poll').hide(500);
		}

		poll.find('.polls, .poll_max_votes, .poll_vote').show(500, function () {
			poll.find('.vote_option').removeClass().addClass('col-md-3 col-md-offset-1 vote_option');
			poll.find('.vote_progress, .poll_option_percent, .poll_total_votes').hide().removeClass('hidden').show(500);
		});

		// Get the votes count of the highest poll option
		poll.find('[data-poll-option-id]').each(function() {
			var option = $(this);
			var optionId = option.attr('data-poll-option-id');
			mostVotes = (res.vote_counts[optionId] >= mostVotes) ? res.vote_counts[optionId] : mostVotes;
		});

		// Update the total votes count
		poll.find('.poll_total_vote_cnt').html(res.total_votes);

		// Update each option
		poll.find('[data-poll-option-id]').each(function() {
			var $this = $(this);
			var optionId = $this.attr('data-poll-option-id');
			var voted = (typeof res.user_votes[optionId] !== 'undefined');
			var mostVoted = (res.vote_counts[optionId] === mostVotes);
			var percent = (!res.total_votes) ? 0 : Math.round((res.vote_counts[optionId] / res.total_votes) * 100);
			var percentRel = (mostVotes === 0) ? 0 : Math.round((res.vote_counts[optionId] / mostVotes) * 100);
			var altText;

			altText = $this.attr('data-alt-text');
			if (voted) {
				$this.attr('title', $.trim(altText));
			} else {
				$this.attr('title', '');
			};
			$this.toggleClass('voted', voted);
			$this.toggleClass('most-votes', mostVoted);

			// Update the bars
			var bar = $this.find('.poll_option_percent');
			var barTimeLapse = (res.can_vote) ? 500 : 1500;
			var newBarClass = (percent < 33) ? 'danger' : (percent < 66) ? 'warning' : 'success';

			setTimeout(function () {
				bar.html(res.vote_counts[optionId]);

				var percentText = percent ? percent + '%' : res.NO_VOTES;
				$this.find('.progress-bar').removeClass().addClass('progress-bar progress-bar-'+newBarClass).css('width',percent ? percent + '%' : '10%').html(percentText);
			}, barTimeLapse);
		});

		if (!res.can_vote) {
			poll.find('.polls').delay(400).fadeIn(500);
		}

		// Display "Your vote has been cast." message.
		var confirmationDelay = (res.can_vote) ? 300 : 900;
		poll.find('.vote-submitted').delay(confirmationDelay).hide().removeClass('hidden').slideDown(200);

	}
});

/**
 * Show poll results when clicking View results link.
 */
$('a.poll_view_results').click(function(e) {
	// Do not follow the link
	e.preventDefault();

	var $poll = $(this).parents('.topic_poll');

	$poll.find('.vote_option').removeClass().addClass('col-md-3 vote_option');
	$poll.find('.vote_progress, .poll_option_percent, .poll_total_votes').hide().removeClass('hidden').show(500);
	$poll.find('.poll_view_results').hide(500);
});

$('[data-ajax]').each(function() {
	var $this = $(this);
	var ajax = $this.attr('data-ajax');
	var filter = $this.attr('data-filter');

	if (ajax !== 'false') {
		var fn = (ajax !== 'true') ? ajax : null;
		filter = (filter !== undefined) ? phpbb.getFunctionByName(filter) : null;

		phpbb.ajaxify({
			selector: this,
			refresh: $this.attr('data-refresh') !== undefined,
			filter: filter,
			callback: fn
		});
	}
});


/**
 * This simply appends #preview to the action of the
 * QR action when you click the Full Editor & Preview button
 */
$('#qr_full_editor').click(function() {
	$('#qr_postform').attr('action', function(i, val) {
		return val + '#preview';
	});
});


/**
 * Make the display post links to use JS
 */
$('.display_post').click(function(e) {
	// Do not follow the link
	e.preventDefault();

	var postId = $(this).attr('data-post-id');
	$('#post_content' + postId).show();
	$('#profile' + postId).show();
	$('#post_mcp_action' + postId).show();
	$('#post_deleted_msg' + postId).show();
	$('#post_hidden' + postId).hide();
});

/**
* Toggle the member search panel in memberlist.php.
*
* If user returns to search page after viewing results the search panel is automatically displayed.
* In any case the link will toggle the display status of the search panel and link text will be
* appropriately changed based on the status of the search panel.
*/
$('#member_search').click(function () {
	var $memberlistSearch = $('#memberlist_search');

	$memberlistSearch.slideToggle('fast');
	phpbb.ajaxCallbacks.alt_text.call(this);

	// Focus on the username textbox if it's available and displayed
	if ($memberlistSearch.is(':visible')) {
		$('#username').focus();
	}
	return false;
});

/**
* Automatically resize textarea
*/

})(jQuery); // Avoid conflicts with other libraries