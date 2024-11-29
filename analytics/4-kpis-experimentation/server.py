from flask import Flask, jsonify, request, render_template_string
from statsig import statsig
from statsig.statsig_event import StatsigEvent
from statsig.statsig_user import StatsigUser
import random
import os

API_KEY = os.environ.get('STATSIG_API_KEY')
statsig.initialize(API_KEY)
app = Flask(__name__)


@app.route('/')
def hello():
    return "Hello, this is a Flask API!"


def signup_page(
        user_id,
        bg_color='#FFFFFF',
        text_color='#000000',
        button_text='Signup'):
    template = """
    <H1>Signup Page</H1>
    <p>Your user_id is {{ user_id }}.</p>
    <p>If you're feeling generous, please Signup by clicking the button.</p>
    <button id="logEventButton" style="background-color: {{ bg_color }}; color: {{ text_color }};">{{ button_text }}</button>
    <script>
    document.getElementById('logEventButton').addEventListener('click', function() {
        fetch('/signup_success', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ user_id: '{{ user_id }}' })
        });
    });
    </script>
    """
    return render_template_string(
        template,
        user_id=user_id,
        bg_color=bg_color,
        text_color=text_color,
        button_text=button_text,
        )


@app.route('/signup')
def signup():
    hash_string = request.remote_addr
    user_id = str(hash(hash_string))
    return signup_page(user_id)


@app.route('/signup_success', methods=['POST'])
def log_event():
    data = request.json
    user_id = data['user_id']
    statsig_user = StatsigUser(user_id)
    statsig_event = StatsigEvent(
        user=statsig_user,
        event_name='signup_success'
    )
    statsig.log_event(statsig_event)
    statsig.flush()
    return '', 204


@app.route('/upsell', methods=['GET'])
def upsell():
    user_id = random.randint(0, 1000000)
    statsig_user = StatsigUser(user_id)
    color = statsig.get_experiment(statsig_user, "pretty_button").get("color", "#FFFFFF")
    button_text = statsig.get_experiment(statsig_user, "pretty_button").get("text", "Sign Up")
    text_color = statsig.get_experiment(statsig_user, "pretty_button").get("text_color", "#000000")
    return signup_page(
        user_id,
        bg_color=color,
        text_color=text_color,
        button_text=button_text
    )


if __name__ == '__main__':
    app.run(debug=True)