import http from 'k6/http';
import { check } from 'k6';

// Cấu hình: 200 user nã đạn liên tục, không ngừng nghỉ trong 15 giây
export const options = {
    vus: 200,
    duration: '15s',
};

const events = ["view", "click", "add_to_cart", "purchase"]
export default function () {
    const payload = JSON.stringify({
        user_id: "user_" + Math.floor(Math.random() * 1000),
        session_id: "session_" + Math.floor(Math.random() * 1000),
        item_id: "item_abc",
        event_type: events[Math.floor(Math.random() * events.length)]
    });

    const params = {
        headers: { 'Content-Type': 'application/json' },
    };

    // Bắn vào Golang API
    const res = http.post('http://localhost:8002/track', payload, params);

    // Kiểm tra xem Go có trả về 200 OK không
    check(res, { 'status is 200': (r) => r.status === 200 });
}