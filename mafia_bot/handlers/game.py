# Extracted from the original mafia_bot/handlers.py.
# Stage 1 refactor: gameplay behavior is intentionally unchanged.
from ._context import *  # noqa: F401,F403

def get_phase_lock(chat_id: int) -> asyncio.Lock:
    lock = phase_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        phase_locks[chat_id] = lock
    return lock


def cancel_phase_timer(chat_id: int) -> None:
    timer = phase_timers.get(chat_id)
    if timer is None:
        return

    current = asyncio.current_task()
    if timer is current:
        # The timer callback reached phase end itself; avoid self-cancel that aborts transition.
        phase_timers.pop(chat_id, None)
        return

    phase_timers.pop(chat_id, None)
    timer.cancel()


async def process_night_end(bot: Bot, chat_id: int, timer_reason: str | None = None) -> None:
    lock = get_phase_lock(chat_id)
    async with lock:
        room = storage.get_room(chat_id)
        if room is None or room.phase != "night":
            return

        cancel_phase_timer(chat_id)
        skipped_user_ids = night_skipped_user_ids(room)
        await mark_skipped_night_menus(bot, room, skipped_user_ids)
        (
            ok,
            info,
            eliminated,
            don_transfer_note,
            don_successor_id,
            commissar_transfer_note,
            commissar_successor_id,
        ) = room.resolve_night()
        if not ok:
            await bot.send_message(chat_id, info)
            return

        if timer_reason:
            try:
                await bot.send_message(chat_id, timer_reason)
            except Exception as e:
                print(f"[ERROR] process_night_end: failed to send timer_reason for chat_id={chat_id}, error={e!r}")

        reports = room.pop_night_reports()
        kill_sources = room.pop_night_kill_sources()
        afk_killed_ids = set(room.afk_killed_user_ids)
        lucky_triggered = False
        for user_id, lines in reports.items():
            try:
                for message_text in compact_night_report_messages(lines):
                    if "пытались убить, но тебе повезло" in message_text.lower():
                        lucky_triggered = True
                    await safe_send_message(bot, user_id, message_text, **private_game_send_kwargs(room))
            except Exception:
                continue

        pending_sergeant_check = room.pop_pending_sergeant_check()
        if pending_sergeant_check is not None:
            sergeant = next(
                (player for player in room.alive_players() if player.role == ROLE_SERGEANT),
                None,
            )
            if sergeant is not None:
                target_user_id = int(pending_sergeant_check["target_user_id"])
                result_role = str(pending_sergeant_check["result_role"])
                try:
                    await safe_send_message(
                        bot,
                        sergeant.user_id,
                        sergeant_commissar_check_text(room, target_user_id, result_role),
                        **private_game_send_kwargs(room),
                    )
                except Exception:
                    pass

        spent_shield_user_ids = room.pop_spent_shield_user_ids()
        shield_triggered = False
        for user_id in spent_shield_user_ids:
            if repo.consume_shield_buff(user_id):
                shield_triggered = True

        spent_documents_user_ids = room.pop_spent_documents_user_ids()
        for user_id in spent_documents_user_ids:
            repo.consume_documents_buff(user_id)

        if shield_triggered:
            await bot.send_message(chat_id, "🌟 Кто-то из игроков потратил защиту")

        if lucky_triggered:
            await bot.send_message(chat_id, "☝️ Кому-то из игроков повезло")

        await send_phase_media(bot, chat_id, room.day_media_caption(), DAY_IMAGE_PATH)

        if eliminated:
            show_killers = bool(room_chat_settings(room).get("misc", {}).get("show_killers", False))
            show_roles = show_roles_enabled(room)
            for dead in eliminated:
                sources = kill_sources.get(dead.user_id, [])
                killer_text = format_killer_sources_text(sources) if show_killers else ""
                dead_mark = room_player_mark(room, dead)
                if show_roles:
                    role_text = role_mark_text(dead.role)
                    text = f"Сегодня был жестоко убит {role_text} {dead_mark}"
                else:
                    text = f"Сегодня был жестоко убит {dead_mark}"
                if killer_text:
                    text += f"\n{killer_text}"
                await bot.send_message(chat_id, text, parse_mode="HTML")
            if room.phase != PHASE_FINISHED:
                non_afk_eliminated = [player for player in eliminated if player.user_id not in afk_killed_ids]
                await prompt_last_words(bot, room, non_afk_eliminated)
            else:
                for dead in eliminated:
                    if dead.user_id in afk_killed_ids:
                        continue
                    try:
                        await safe_send_message(
                            bot,
                            dead.user_id,
                            "<b>Тебя убили :(</b>",
                            **private_game_send_kwargs(room),
                        )
                    except Exception:
                        pass

            for dead in eliminated:
                if dead.user_id not in afk_killed_ids:
                    continue
                try:
                    await safe_send_message(
                        bot,
                        dead.user_id,
                        "Ты бездействовал больше 2 ночей подряд и был убит...",
                        **private_game_send_kwargs(room),
                    )
                except Exception:
                    pass
                dead_mark = room_player_mark(room, dead)
                public_prefix = f"Кто-то из жителей слышал, как {dead_mark} кричал перед смертью:\n"
                await safe_send_message(
                    bot,
                    chat_id,
                    f"{public_prefix}<b>Я больше не бу-у-у-у-ду спать во время игры-ы-ы-ы-ы-ы-!</b>",
                    parse_mode="HTML",
                )
        else:
            await bot.send_message(chat_id, "🤷 Удивительно, но этой ночью все выжили")

        if don_transfer_note:
            await announce_don_transfer(room, bot, don_successor_id)
        if commissar_transfer_note:
            await announce_commissar_transfer(room, bot, commissar_successor_id)

        room.afk_killed_user_ids.clear()

        day_summary = (
            room.alive_players_text()
            + "\n\n"
            + room.alive_role_hints_text()
            + "\n\n"
            + "Сейчас самое время обсудить результаты ночи, разобраться в причинах и следствиях..."
        )
        await bot.send_message(chat_id, day_summary, parse_mode="HTML")

        if room.phase == PHASE_FINISHED:
            room.pending_last_words.clear()
            stats_already_recorded = room.stats_recorded
            ensure_stats_recorded(room)
            if not stats_already_recorded:
                await send_endgame_currency_summaries(bot, room)
            await bot.send_message(chat_id, room.final_report_text(), parse_mode="HTML")
            cancel_phase_timer(chat_id)
            persist_room(room)
            return

        room.start_day_discussion()
        await start_phase_timer(room, bot)
        persist_room(room)


async def process_day_end(bot: Bot, chat_id: int, timer_reason: str | None = None) -> None:

    lock = get_phase_lock(chat_id)
    async with lock:
        print(f"[PHASE] process_day_end called for chat_id={chat_id}")
        room = storage.get_room(chat_id)
        if room is None:
            print(f"[PHASE] process_day_end: room is None for chat_id={chat_id}")
            return
        if room.phase != "day":
            print(f"[PHASE] process_day_end: phase is not 'day' (actual: {room.phase}) for chat_id={chat_id}")
            return

        print(f"[PHASE] process_day_end: current day_stage={room.day_stage}")
        cancel_phase_timer(chat_id)

        if timer_reason:
            print(f"[PHASE] process_day_end: timer_reason={timer_reason}")
            try:
                await bot.send_message(chat_id, timer_reason)
            except Exception as e:
                print(f"[ERROR] process_day_end: failed to send timer_reason for chat_id={chat_id}, error={e!r}")

        if room.day_stage == DAY_STAGE_DISCUSSION:
            print(f"[PHASE] process_day_end: switching to nomination stage for chat_id={chat_id}")
            room.start_day_nomination()
            persist_room(room)
            private_keyboard = await night_action_keyboard(bot)
            await bot.send_message(
                chat_id,
                (
                    "<b>Пришло время определить и наказать виновных.</b>\n"
                    f"Голосование продлится {int(room_chat_settings(room)['timings']['vote'])} секунд"
                ),
                reply_markup=private_keyboard,
            )
            await push_phase_action_menus(bot, room)
            await start_phase_timer(room, bot)
            print(f"[PHASE] process_day_end: nomination stage started, timer set for chat_id={chat_id}")
            return

        if room.day_stage == DAY_STAGE_NOMINATION:
            ok, candidate_id = room.resolve_day_nomination()
            print(f"[PHASE] process_day_end: nomination resolved for chat_id={chat_id}, ok={ok}, candidate_id={candidate_id}")
            if not ok:
                print(f"[PHASE] process_day_end: nomination resolve failed for chat_id={chat_id}")
                await bot.send_message(chat_id, "Не удалось обработать выбор кандидата.")
                return

            if candidate_id is None:
                print(f"[PHASE] process_day_end: no single candidate selected, ending day without lynch for chat_id={chat_id}")
                actual_votes = [
                    target_id
                    for target_id in room.day_votes.values()
                    if target_id and (room.get_player(target_id) is not None)
                ]
                if actual_votes:
                    await bot.send_message(
                        chat_id,
                        "Голоса на этапе выбора кандидата разделились поровну.\n"
                        "🗿 Жители решили никого не вешать...",
                    )
                else:
                    await bot.send_message(
                        chat_id,
                        "Голосование окончено\n🗿 Жители решили никого не вешать...",
                    )

                ok_end, info_end = room.end_day_no_lynch()
                print(f"[PHASE] process_day_end: end_day_no_lynch result for chat_id={chat_id}, ok_end={ok_end}, info={info_end}")
                if ok_end:
                    keyboard = await night_action_keyboard(bot)
                    await send_phase_media(
                        bot,
                        chat_id,
                        room.night_media_caption(),
                        NIGHT_IMAGE_PATH,
                        reply_markup=keyboard,
                    )
                    await bot.send_message(
                        chat_id,
                        night_status_text(room),
                        reply_markup=keyboard,
                    )
                    await push_phase_action_menus(bot, room)
                    await push_kamikaze_revenge_menu(bot, room)
                    await start_phase_timer(room, bot)
                    persist_room(room)
                    print(f"[PHASE] process_day_end: transitioned to night after no-lynch for chat_id={chat_id}")
                else:
                    await bot.send_message(chat_id, info_end)
                return

            candidate = room.get_player(candidate_id)
            if candidate is None:
                print(f"[PHASE] process_day_end: candidate_id={candidate_id} not found, forcing no-lynch for chat_id={chat_id}")
                await bot.send_message(chat_id, "Кандидат не найден. День завершается без повешения.")
                ok_end, _ = room.end_day_no_lynch()
                if ok_end:
                    keyboard = await night_action_keyboard(bot)
                    await send_phase_media(
                        bot,
                        chat_id,
                        room.night_media_caption(),
                        NIGHT_IMAGE_PATH,
                        reply_markup=keyboard,
                    )
                    await bot.send_message(
                        chat_id,
                        night_status_text(room),
                        reply_markup=keyboard,
                    )
                    await push_phase_action_menus(bot, room)
                    await push_kamikaze_revenge_menu(bot, room)
                    await start_phase_timer(room, bot)
                    persist_room(room)
                    print(f"[PHASE] process_day_end: transitioned to night after missing candidate fallback for chat_id={chat_id}")
                return

            room.start_day_trial(candidate.user_id)
            print(f"[PHASE] process_day_end: trial started for chat_id={chat_id}, candidate_id={candidate.user_id}, candidate_name={candidate.full_name}")
            persist_room(room)
            await push_trial_vote_menus(bot, room, candidate)
            await start_phase_timer(room, bot)
            print(f"[PHASE] process_day_end: trial menus sent and timer started for chat_id={chat_id}")
            return

        if room.day_stage == DAY_STAGE_TRIAL:
            yes_count, no_count = room.trial_vote_counts()
            candidate = room.get_player(room.trial_candidate_id) if room.trial_candidate_id is not None else None
            await finish_trial_vote_message(bot, room, candidate, yes_count, no_count)
            print(f"[PHASE] process_day_end: resolving trial for chat_id={chat_id}, yes_count={yes_count}, no_count={no_count}, votes={room.trial_votes}")
            (
                ok,
                info,
                eliminated,
                don_transfer_note,
                don_successor_id,
                commissar_transfer_note,
                commissar_successor_id,
            ) = room.resolve_day_trial()
            if not ok:
                print(f"[PHASE] process_day_end: trial resolve failed for chat_id={chat_id}, info={info}")
                await safe_send_message(bot, chat_id, info)
                return

            if eliminated:
                first = eliminated[0]
                first_mark = room_player_mark(room, first)
                verdict_target = "обвиняемого" if is_secret_voting_enabled(room) else first_mark
                reveal_roles = show_roles_enabled(room)
                await safe_send_message(
                    bot,
                    chat_id,
                    f"<b>Результаты голосования:</b>\n<b>{yes_count}</b> 👍  |  <b>{no_count}</b> 👎\n\nВешаем {verdict_target}! :)",
                    parse_mode="HTML",
                )
                if reveal_roles:
                    role_text = role_mark_text(first.role)
                    await asyncio.sleep(2)
                    await safe_send_message(bot, chat_id, f"{first_mark} был {role_text}", parse_mode="HTML")
                await asyncio.sleep(2)
                try:
                    if first.role == ROLE_KAMIKAZE:
                        await safe_send_message(
                            bot,
                            first.user_id,
                            "Тебя линчевали на дневном собрании :(\nКого заберём с собой в могилу?",
                        )
                    else:
                        await safe_send_message(
                            bot,
                            first.user_id,
                            "тебя линчевали на дневном голосовании",
                        )
                except Exception:
                    pass
                if first.role == "Самоубийца":
                    await safe_send_message(bot, chat_id, "💀 <b>Самоубийца</b> выполнил личную цель победы.")
                if first.role == "Камикадзе" and len(eliminated) > 1:
                    second = eliminated[1]
                    second_mark = room_player_mark(room, second)
                    if reveal_roles:
                        second_role = role_mark_text(second.role)
                        await safe_send_message(bot, chat_id, f"💣 Камикадзе забрал с собой {second_mark} ({second_role}).", parse_mode="HTML")
                    else:
                        await safe_send_message(bot, chat_id, f"💣 Камикадзе забрал с собой {second_mark}.", parse_mode="HTML")
            else:
                await safe_send_message(
                    bot,
                    chat_id,
                    "Мнения жителей разошлись\n"
                    f"(<b>{yes_count}</b> 👍 | <b>{no_count}</b> 👎 )... Разошлись и сами жители, так никого и не повесив...",
                )

            if don_transfer_note:
                await announce_don_transfer(room, bot, don_successor_id)
            if commissar_transfer_note:
                await announce_commissar_transfer(room, bot, commissar_successor_id)

            if room.phase == PHASE_FINISHED:
                room.pending_last_words.clear()
                stats_already_recorded = room.stats_recorded
                ensure_stats_recorded(room)
                if not stats_already_recorded:
                    await send_endgame_currency_summaries(bot, room)
                await safe_send_message(bot, chat_id, room.final_report_text(), parse_mode="HTML")
                cancel_phase_timer(chat_id)
                persist_room(room)
                return

            await asyncio.sleep(2)
            keyboard = await night_action_keyboard(bot)
            await send_phase_media(
                bot,
                chat_id,
                room.night_media_caption(),
                NIGHT_IMAGE_PATH,
                reply_markup=keyboard,
            )
            await safe_send_message(
                bot,
                chat_id,
                night_status_text(room),
                reply_markup=keyboard,
            )
            await push_phase_action_menus(bot, room)
            await push_kamikaze_revenge_menu(bot, room)
            await start_phase_timer(room, bot)
            persist_room(room)
            return

        print(f"[ERROR] process_day_end: unknown day_stage for chat_id={chat_id}, day_stage={room.day_stage}, phase={room.phase}")
        await bot.send_message(chat_id, "Не удалось определить текущий этап дня.")


async def phase_timer_worker(bot: Bot, chat_id: int, phase: str, duration_sec: int) -> None:
    try:
        await asyncio.sleep(duration_sec)
        room = storage.get_room(chat_id)
        if room is None or room.phase != phase:
            return

        if phase == "night":
            await process_night_end(bot, chat_id, timer_reason=None)
        elif phase == "day":
            await process_day_end(bot, chat_id, timer_reason=None)
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(f"[ERROR] phase_timer_worker: chat_id={chat_id}, phase={phase}, error={e!r}")


async def start_phase_timer(
    room,
    bot: Bot,
    remaining_sec: int | None = None,
    reset_deadline: bool = True,
) -> None:
    cancel_phase_timer(room.chat_id)

    settings = room_chat_settings(room)
    phase_duration = int(settings["timings"]["night"])
    if room.phase == "night":
        phase_duration = int(settings["timings"]["night"])
    elif room.phase == "day":
        phase_duration = current_day_stage_seconds(room)
    else:
        return

    if remaining_sec is None:
        remaining_sec = phase_duration
    if remaining_sec <= 0:
        remaining_sec = 1

    if reset_deadline:
        room.phase_started_at = datetime.now()
        room.phase_duration_seconds = phase_duration
    else:
        if room.phase_started_at is None:
            room.phase_started_at = datetime.now()
        if room.phase_duration_seconds is None:
            room.phase_duration_seconds = phase_duration

    phase_timers[room.chat_id] = asyncio.create_task(
        phase_timer_worker(bot, room.chat_id, room.phase, remaining_sec)
    )
    persist_room(room)


async def restore_runtime_state(bot: Bot) -> None:
    for room in storage.rooms.values():
        apply_room_settings(room, room.settings)
        if not room.started and room.registration_open:
            remaining = registration_remaining_seconds(room)
            if remaining <= 0:
                await process_registration_timeout(bot, room.chat_id)
            else:
                await start_registration_timer(room, bot, remaining)
            continue

        if not room.started or room.phase in {PHASE_FINISHED, "lobby"}:
            continue
        if room.phase == PHASE_DAY and room.day_stage is None:
            room.start_day_discussion()
            persist_room(room)

        base_duration = room.phase_duration_seconds
        if base_duration is None or base_duration <= 0:
            if room.phase == PHASE_NIGHT:
                base_duration = int(room_chat_settings(room)["timings"]["night"])
            else:
                base_duration = current_day_stage_seconds(room)

        remaining_sec: int | None = None
        if room.phase_started_at is not None:
            elapsed = int((datetime.now() - room.phase_started_at).total_seconds())
            remaining_sec = base_duration - elapsed

        if remaining_sec is not None and remaining_sec <= 0:
            if RESTART_EXPIRED_PHASE_POLICY == "restart":
                # Restart the current phase from a full timer after reboot instead of auto-closing it.
                remaining_sec = base_duration
                room.phase_started_at = datetime.now()
                room.phase_duration_seconds = base_duration
                persist_room(room)
                print(
                    f"[PHASE] restore_runtime_state: expired phase restarted for chat_id={room.chat_id}, "
                    f"phase={room.phase}, duration={remaining_sec}s"
                )
            else:
                if room.phase == PHASE_NIGHT:
                    await process_night_end(
                        bot,
                        room.chat_id,
                        timer_reason=None
                    )
                elif room.phase == PHASE_DAY:
                    await process_day_end(
                        bot,
                        room.chat_id,
                        timer_reason="⏱ Время дня истекло во время перезапуска. Фаза закрыта автоматически."
                    )
                continue

        await start_phase_timer(room, bot, remaining_sec=remaining_sec, reset_deadline=False)
        await push_phase_action_menus(bot, room)


async def maybe_finish_phase_early(bot: Bot, room) -> None:
    if room.phase == "night" and room.all_required_night_actions_done():
        await process_night_end(bot, room.chat_id, timer_reason=None)
        return

    if room.phase == "day" and room.day_stage == DAY_STAGE_NOMINATION and room.all_alive_day_voted():
        await process_day_end(bot, room.chat_id, timer_reason=None)
        return

    if room.phase == "day" and room.day_stage == DAY_STAGE_TRIAL and room.all_alive_trial_voted():
        await process_day_end(bot, room.chat_id, timer_reason=None)

