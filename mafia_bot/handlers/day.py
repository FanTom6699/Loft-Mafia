# Extracted from mafia_bot/handlers.py.
# Refactor only: gameplay behavior is unchanged.
from ._context import *  # noqa: F401,F403

def trial_vote_prompt_text(room, candidate) -> str:
    if candidate is None:
        return "Вы точно хотите линчевать обвиняемого?"
    return f"Вы точно хотите линчевать {room_player_mark(room, candidate)}?"


def trial_vote_keyboard(chat_id: int, yes_count: int, no_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"👍 {yes_count}", callback_data=f"trial:yes:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text=f"👎 {no_count}", callback_data=f"trial:no:{chat_id}"),
            ],
        ]
    )


async def push_trial_vote_menus(bot: Bot, room, candidate) -> None:
    candidate_name = player_display_name(candidate)
    print(f"[TRIAL] push_trial_vote_menus: candidate={candidate_name}, chat_id={room.chat_id}")
    yes_count, no_count = room.trial_vote_counts()
    try:
        sent = await bot.send_message(
            room.chat_id,
            trial_vote_prompt_text(room, candidate),
            reply_markup=trial_vote_keyboard(room.chat_id, yes_count, no_count),
            parse_mode="HTML",
        )
        room.trial_vote_message_id = sent.message_id
        persist_room(room)
    except Exception as e:
        print(f"[ERROR] push_trial_vote_menus: chat_id={room.chat_id}, error={e!r}")


async def finish_trial_vote_message(bot: Bot, room, candidate, yes_count: int, no_count: int) -> None:
    message_id = room.trial_vote_message_id
    if message_id is None:
        return

    completion_text = f"{trial_vote_prompt_text(room, candidate)}\n\nГолосование завершено"
    try:
        await bot.edit_message_text(
            completion_text,
            chat_id=room.chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception:
        try:
            await bot.edit_message_reply_markup(
                chat_id=room.chat_id,
                message_id=message_id,
                reply_markup=None,
            )
        except Exception:
            pass
    room.trial_vote_message_id = None
    persist_room(room)


async def prompt_last_words(bot: Bot, room, eliminated) -> None:
    if room.phase == PHASE_FINISHED:
        return
    queued_user_ids = room.queue_last_words(eliminated)
    if queued_user_ids:
        persist_room(room)
    for user_id in queued_user_ids:
        try:
            await bot.send_message(
                user_id,
                (
                    "<b>Тебя убили :(</b>\n"
                    "Ты можешь отправить сюда своё предсмертное сообщение"
                ),
                **private_game_send_kwargs(room),
            )
        except Exception:
            continue


@router.callback_query(F.data.startswith("trial:"))
async def on_trial_callback(callback: CallbackQuery) -> None:
    try:
        if callback.message is None or callback.from_user is None:
            return

        parts = callback.data.split(":")
        if len(parts) != 3:
            await callback.answer("Некорректное голосование. Попробуй снова.", show_alert=True)
            return

        _, raw_vote, raw_chat_id = parts
        if raw_vote not in {"yes", "no"}:
            await callback.answer("Некорректный вариант голоса. Попробуй снова.", show_alert=True)
            return

        try:
            chat_id = int(raw_chat_id)
        except ValueError:
            await callback.answer("Некорректный чат. Попробуй снова.", show_alert=True)
            return

        should_finish_trial = False
        lock = get_phase_lock(chat_id)
        async with lock:
            room = storage.get_room(chat_id)
            if room is None or room.phase != PHASE_DAY or room.day_stage != DAY_STAGE_TRIAL:
                await callback.answer("Сейчас нет активного голосования за/против. Дождись начала повешения.", show_alert=True)
                return

            if callback.message.chat.id != room.chat_id:
                await callback.answer("Голосование проходит в групповом чате. Проголосуй в чате игры.", show_alert=True)
                return

            if room.trial_vote_message_id is not None and callback.message.message_id != room.trial_vote_message_id:
                await callback.answer("Это голосование уже завершено. Дождись следующего этапа.", show_alert=True)
                return

            voter = room.get_player(callback.from_user.id)
            if voter is None or not voter.alive:
                await callback.answer(MSG_NOT_IN_GAME)
                return

            approve = raw_vote == "yes"
            ok, info = room.set_trial_vote(callback.from_user.id, approve)
            if not ok:
                if info == MISTRESS_DAY_BLOCK_TOAST:
                    await callback.answer(MISTRESS_DAY_BLOCK_TOAST)
                    return
                await callback.answer(info, show_alert=True)
                return

            await callback.answer(info)
            persist_room(room)

            yes_count, no_count = room.trial_vote_counts()
            candidate = room.get_player(room.trial_candidate_id) if room.trial_candidate_id is not None else None

            candidate_id = room.trial_candidate_id
            eligible_voter_ids = {
                player.user_id
                for player in room.alive_players()
                if candidate_id is None or player.user_id != candidate_id
            }
            received_vote_ids = {user_id for user_id in room.trial_votes if user_id in eligible_voter_ids}
            trial_complete = received_vote_ids == eligible_voter_ids

            if room.all_alive_trial_voted() or trial_complete:
                print(
                    "[TRIAL] voting_complete "
                    f"chat_id={room.chat_id}, eligible={len(eligible_voter_ids)}, received={len(received_vote_ids)}, "
                    f"yes={yes_count}, no={no_count}"
                )
                await finish_trial_vote_message(callback.bot, room, candidate, yes_count, no_count)
                should_finish_trial = True
            else:
                try:
                    await callback.message.edit_text(
                        trial_vote_prompt_text(room, candidate),
                        reply_markup=trial_vote_keyboard(chat_id, yes_count, no_count),
                        parse_mode="HTML",
                    )
                except Exception:
                    try:
                        await callback.message.edit_reply_markup(reply_markup=trial_vote_keyboard(chat_id, yes_count, no_count))
                    except Exception:
                        pass

        if should_finish_trial:
            await process_day_end(callback.bot, chat_id, timer_reason=None)
    except Exception as e:
        print(f"[ERROR] on_trial_callback: chat_id={getattr(getattr(callback, 'message', None), 'chat', None) and callback.message.chat.id}, error={e!r}")
        print(traceback.format_exc())
        try:
            await callback.answer("Произошла ошибка при голосовании. Попробуй еще раз.", show_alert=True)
        except Exception:
            pass
